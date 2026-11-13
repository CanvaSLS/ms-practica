"""ms-practica (OPCIONAL / módulo "wow" extra) · quiz de repaso por curso.
PRODUCTOR de QuizCompletado y CONSUMIDOR de ModuloPublicado (siembra 3 tarjetas por módulo).

Tablas:
  sp_tarjetas  PK tenant_id · SK curso_tarjeta ("C0001#K00001")
  sp_intentos  PK tenant_id · SK alumno_ts     ("A00001#2026-11-12T10:00:00Z")
La corrección se hace en el SERVIDOR: la respuesta correcta nunca viaja en GET /practica/quiz.
"""
import random
import uuid

from boto3.dynamodb.conditions import Key

from common import (ApiError, body, exigir_rol, log, now_iso, procesar_lote, protegido,
                    publicar_evento, query_params, query_todo, requerido, tabla, verificar_tenant)

TARJETAS = tabla("T_TARJETAS")
INTENTOS = tabla("T_INTENTOS")
TAMANO_QUIZ = 5


def _tarjetas_curso(tenant_id, curso_id):
    return query_todo(TARJETAS, maximo=300, KeyConditionExpression=(
        Key("tenant_id").eq(tenant_id) & Key("curso_tarjeta").begins_with(curso_id + "#")))


@protegido
def obtener_quiz(event, ctx):
    exigir_rol(ctx, "alumno")
    qs = query_params(event)
    verificar_tenant(ctx, qs)
    curso_id = requerido(qs, "curso_id", 20)
    tarjetas = _tarjetas_curso(ctx["tenant_id"], curso_id)
    if not tarjetas:
        raise ApiError(404, "SIN_TARJETAS", "Este curso aún no tiene tarjetas de práctica")
    elegidas = random.sample(tarjetas, min(TAMANO_QUIZ, len(tarjetas)))
    return {"curso_id": curso_id, "preguntas": [
        {"tarjeta_id": t["tarjeta_id"], "pregunta": t["pregunta"], "opciones": t["opciones"],
         "tema": t.get("tema", "")} for t in elegidas]}


@protegido
def completar_quiz(event, ctx):
    exigir_rol(ctx, "alumno")
    d = body(event)
    verificar_tenant(ctx, d)
    curso_id = requerido(d, "curso_id", 20)
    respuestas = d.get("respuestas")
    if not isinstance(respuestas, list) or not respuestas:
        raise ApiError(400, "RESPUESTAS_REQUERIDAS", "Envía la lista de respuestas")
    aciertos, detalle = 0, []
    for r in respuestas[:TAMANO_QUIZ]:
        t = TARJETAS.get_item(Key={"tenant_id": ctx["tenant_id"],
                                   "curso_tarjeta": f"{curso_id}#{r.get('tarjeta_id')}"}).get("Item")
        if not t:
            raise ApiError(404, "TARJETA_NO_ENCONTRADA", "Tarjeta inexistente en tu tenant")
        ok = int(r.get("opcion", -1)) == int(t["correcta"])
        aciertos += int(ok)
        detalle.append({"tarjeta_id": t["tarjeta_id"], "correcta": int(t["correcta"]), "acerto": ok})
    total = len(detalle)
    ts = now_iso()
    INTENTOS.put_item(Item={
        "tenant_id": ctx["tenant_id"], "alumno_ts": f"{ctx['user_id']}#{ts}", "alumno_id": ctx["user_id"],
        "curso_id": curso_id, "aciertos": aciertos, "total": total, "creado_en": ts})
    publicar_evento("QuizCompletado", ctx["tenant_id"], {
        "alumno_id": ctx["user_id"], "curso_id": curso_id, "aciertos": aciertos, "total": total})
    return {"aciertos": aciertos, "total": total, "detalle": detalle}


def _sembrar(msg):
    """Consumidor de ModuloPublicado: crea 3 tarjetas base para el módulo nuevo."""
    if msg["evento"] != "ModuloPublicado":
        return
    t, d = msg["tenant_id"], msg["detalle"]
    with TARJETAS.batch_writer(overwrite_by_pkeys=["tenant_id", "curso_tarjeta"]) as bw:
        for i in range(3):
            tid = f"K{d['modulo_id']}{i}"  # determinista: reintentos no duplican
            bw.put_item(Item={
                "tenant_id": t, "curso_tarjeta": f"{d['curso_id']}#{tid}", "tarjeta_id": tid,
                "curso_id": d["curso_id"], "modulo_id": d["modulo_id"], "tema": d["titulo"],
                "pregunta": f"({i + 1}/3) ¿Cuál es una idea clave de «{d['titulo']}»?",
                "opciones": ["Idea A", "Idea B", "Idea C", "Idea D"], "correcta": 0, "dificultad": 1})
    log("INFO", accion="tarjetas_sembradas", tenant_id=t, modulo_id=d["modulo_id"])


def sembrar_tarjetas(event, context):
    return procesar_lote(event, _sembrar)
