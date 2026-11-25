"""Utilidades compartidas por los microservicios de CanvaSLS.

Cada microservicio vive en su propio repositorio, por eso este archivo se COPIA
(idéntico) en ms-auth, ms-cursos, ms-evaluaciones, ms-analitica,
ms-notificaciones y ms-practica. Si lo modifican, copien el cambio a todos.

Ideas del curso que se aplican aquí:
  - Semana 9  : DynamoDB con tenant_id como clave de partición (multi-tenancy).
  - Semana 10 : seguridad por token con expiración (el tenant NUNCA se acepta del cliente).
  - Semana 11 : logs JSON estructurados para CloudWatch Logs Insights.
  - Semana 13 : SNS (publicar) y 14: SQS + DLQ (consumir con reintentos).
"""
import base64
import decimal
import functools
import json
import os
import uuid
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
    "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
}


class ApiError(Exception):
    def __init__(self, status, codigo, mensaje=""):
        super().__init__(codigo)
        self.status, self.codigo, self.mensaje = status, codigo, mensaje


class _Enc(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return int(o) if o == o.to_integral_value() else float(o)
        if isinstance(o, (set, frozenset)):
            return sorted(o)
        return super().default(o)


def dumps(obj):
    return json.dumps(obj, cls=_Enc, ensure_ascii=False)


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(tipo, **datos):
    """Log JSON estructurado (una línea) -> consultable con CloudWatch Logs Insights."""
    print(dumps({"tipo": tipo, **datos}))


# ----------------------------------------------------------------- HTTP / API
def resp(status, data):
    return {"statusCode": status, "headers": HEADERS, "body": dumps(data)}


def body(event):
    raw = event.get("body")
    if not raw:
        return {}
    if event.get("isBase64Encoded"):
        raw = base64.b64decode(raw).decode()
    try:
        data = json.loads(raw, parse_float=decimal.Decimal)  # DynamoDB no acepta float
    except ValueError:
        raise ApiError(400, "JSON_INVALIDO", "El cuerpo no es JSON válido")
    if not isinstance(data, dict):
        raise ApiError(400, "JSON_INVALIDO", "Se esperaba un objeto JSON")
    return data


def query_params(event):
    return event.get("queryStringParameters") or {}


def path_params(event):
    return event.get("pathParameters") or {}


def requerido(d, campo, maximo=200):
    v = d.get(campo)
    if not isinstance(v, str) or not v.strip():
        raise ApiError(400, "CAMPO_REQUERIDO", f"Falta '{campo}'")
    v = v.strip()
    if len(v) > maximo:
        raise ApiError(400, "CAMPO_MUY_LARGO", f"'{campo}' supera {maximo} caracteres")
    return v


# ------------------------------------------------------- Identidad y tenant
def contexto(event):
    """Identidad que el Lambda Authorizer inyectó en API Gateway (nunca viene del cliente)."""
    a = (event.get("requestContext") or {}).get("authorizer") or {}
    if not a.get("tenant_id") or not a.get("user_id"):
        raise ApiError(401, "NO_AUTENTICADO", "Falta el contexto del authorizer")
    return {
        "tenant_id": a["tenant_id"],
        "user_id": a["user_id"],
        "rol": a.get("rol", ""),
        "nombre": a.get("nombre", ""),
    }


def verificar_tenant(ctx, *fuentes):
    """Si el cliente declara un tenant distinto al del token -> 403 y queda auditado."""
    for fuente in fuentes:
        t = (fuente or {}).get("tenant_id")
        if t is not None and t != ctx["tenant_id"]:
            log("SEGURIDAD", evento="TenantMismatch", tenant_token=ctx["tenant_id"],
                tenant_solicitado=str(t), user_id=ctx["user_id"])
            raise ApiError(403, "TENANT_MISMATCH",
                           "El tenant solicitado no coincide con el de tu sesión")


def exigir_rol(ctx, *roles):
    if ctx["rol"] not in roles:
        log("SEGURIDAD", evento="RolNoAutorizado", tenant_id=ctx["tenant_id"],
            user_id=ctx["user_id"], rol=ctx["rol"], requeridos=list(roles))
        raise ApiError(403, "ROL_NO_AUTORIZADO", f"Se requiere rol: {', '.join(roles)}")


def _ejecutar(fn, event, con_contexto):
    ctx = None
    try:
        ctx = contexto(event) if con_contexto else None
        out = fn(event, ctx) if con_contexto else fn(event)
        status, data = out if isinstance(out, tuple) else (200, out)
        log("INFO", ruta=event.get("resource"), metodo=event.get("httpMethod"), status=status,
            tenant_id=(ctx or {}).get("tenant_id"), user_id=(ctx or {}).get("user_id"))
        return resp(status, data)
    except ApiError as e:
        log("WARN", ruta=event.get("resource"), metodo=event.get("httpMethod"), status=e.status,
            codigo=e.codigo, tenant_id=(ctx or {}).get("tenant_id"))
        return resp(e.status, {"error": e.codigo, "mensaje": e.mensaje})
    except Exception as e:  # noqa: BLE001  (error inesperado: no filtrar detalles al cliente)
        log("ERROR", ruta=event.get("resource"), error=repr(e),
            tenant_id=(ctx or {}).get("tenant_id"))
        return resp(500, {"error": "ERROR_INTERNO", "mensaje": "Error inesperado"})


def protegido(fn):
    """Endpoint tras el Lambda Authorizer. fn(event, ctx) -> data | (status, data)."""
    @functools.wraps(fn)
    def handler(event, _context):
        return _ejecutar(fn, event, True)
    return handler


def publico(fn):
    """Endpoint sin authorizer (solo registro/login). fn(event) -> data | (status, data)."""
    @functools.wraps(fn)
    def handler(event, _context):
        return _ejecutar(fn, event, False)
    return handler


# ------------------------------------------------------------------ DynamoDB
def tabla(var_entorno):
    return boto3.resource("dynamodb").Table(os.environ[var_entorno])


def _condicion_fallo(e):
    return e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException"


def update_item(tbl, key, datos, permitidos):
    """UPDATE con lista blanca de campos. 404 si el ítem no existe (nunca crea por accidente)."""
    sets, nombres, valores = [], {}, {}
    campos = [c for c in permitidos if c in datos]
    for i, campo in enumerate(campos):
        sets.append(f"#f{i} = :v{i}")
        nombres[f"#f{i}"] = campo
        valores[f":v{i}"] = datos[campo]
    if not sets:
        raise ApiError(400, "SIN_CAMPOS", "No enviaste ningún campo editable")
    sets.append("actualizado_en = :ts")
    valores[":ts"] = now_iso()
    try:
        r = tbl.update_item(
            Key=key, UpdateExpression="SET " + ", ".join(sets),
            ExpressionAttributeNames=nombres, ExpressionAttributeValues=valores,
            ConditionExpression="attribute_exists(tenant_id)", ReturnValues="ALL_NEW")
    except ClientError as e:
        if _condicion_fallo(e):
            raise ApiError(404, "NO_ENCONTRADO", "El recurso no existe en tu tenant")
        raise
    return r["Attributes"]


def borrar_item(tbl, key):
    try:
        tbl.delete_item(Key=key, ConditionExpression="attribute_exists(tenant_id)")
    except ClientError as e:
        if _condicion_fallo(e):
            raise ApiError(404, "NO_ENCONTRADO", "El recurso no existe en tu tenant")
        raise


def query_todo(tbl, maximo=2000, **kw):
    """Query con paginación interna (siempre Query, nunca Scan)."""
    items = []
    while True:
        r = tbl.query(**kw)
        items += r["Items"]
        if "LastEvaluatedKey" not in r or len(items) >= maximo:
            return items[:maximo]
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]


def contar(tbl, **kw):
    total, kw = 0, dict(kw, Select="COUNT")
    while True:
        r = tbl.query(**kw)
        total += r["Count"]
        if "LastEvaluatedKey" not in r:
            return total
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]


def consulta_paginada(tbl, params, kw, pk=None, limite_max=100):
    """Query paginada con cursor opaco. pk=(atributo, valor) impide cursores de otro tenant."""
    try:
        limite = max(1, min(int(params.get("limit", 25)), limite_max))
    except ValueError:
        raise ApiError(400, "LIMIT_INVALIDO", "limit debe ser un entero")
    kw = dict(kw, Limit=limite)
    cursor = params.get("cursor")
    if cursor:
        try:
            esk = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        except Exception:  # noqa: BLE001
            raise ApiError(400, "CURSOR_INVALIDO", "cursor inválido")
        if pk and esk.get(pk[0]) != pk[1]:
            raise ApiError(403, "CURSOR_AJENO", "El cursor no pertenece a tu tenant")
        kw["ExclusiveStartKey"] = esk
    r = tbl.query(**kw)
    nxt = r.get("LastEvaluatedKey")
    return r["Items"], (base64.urlsafe_b64encode(dumps(nxt).encode()).decode() if nxt else None)


# ---------------------------------------------------------------- SNS / SQS / S3
_sns = None


def publicar_evento(evento, tenant_id, detalle):
    """Publica un hecho de negocio en el topic SNS (pub/sub). Best-effort: no tumba la API."""
    global _sns
    event_id = str(uuid.uuid4())
    mensaje = {"event_id": event_id, "evento": evento, "tenant_id": tenant_id, "ts": now_iso(),
               "origen": os.environ.get("SERVICE_NAME", "desconocido"), "detalle": detalle}
    try:
        _sns = _sns or boto3.client("sns")
        _sns.publish(
            TopicArn=os.environ["TOPIC_ARN"], Subject=evento, Message=dumps(mensaje),
            MessageAttributes={
                "tenant_id": {"DataType": "String", "StringValue": tenant_id},
                "evento": {"DataType": "String", "StringValue": evento},
            })
        log("EVENTO", accion="publicado", evento=evento, event_id=event_id, tenant_id=tenant_id)
    except Exception as e:  # noqa: BLE001
        log("ERROR", accion="publicar_evento", evento=evento, tenant_id=tenant_id, error=repr(e))
    return event_id


def mensaje_sqs(record):
    """Desenvuelve el sobre SNS->SQS y devuelve el evento de negocio."""
    sobre = json.loads(record["body"], parse_float=decimal.Decimal)
    if "Message" in sobre:
        return json.loads(sobre["Message"], parse_float=decimal.Decimal)
    return sobre


def procesar_lote(event, fn):
    """Procesa un lote SQS devolviendo SOLO los mensajes fallidos (ReportBatchItemFailures).
    Tras maxReceiveCount fallos SQS los mueve a la DLQ (patrón de la semana 14)."""
    fallos = []
    for r in event.get("Records", []):
        try:
            fn(mensaje_sqs(r))
        except Exception as e:  # noqa: BLE001
            log("ERROR", accion="consumidor", error=repr(e), message_id=r.get("messageId"),
                intento=(r.get("attributes") or {}).get("ApproximateReceiveCount"))
            fallos.append({"itemIdentifier": r["messageId"]})
    return {"batchItemFailures": fallos}


def url_firmada(operacion, key, content_type=None, expira=300):
    """URL prefirmada de S3 (el archivo viaja directo navegador <-> S3, no por Lambda)."""
    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"),
                      config=Config(signature_version="s3v4"))
    params = {"Bucket": os.environ["BUCKET"], "Key": key}
    if content_type:
        params["ContentType"] = content_type
    return s3.generate_presigned_url(operacion, Params=params, ExpiresIn=expira)
