# ms-practica

(Opcional) Quiz de práctica calificado en el servidor. Siembra tarjetas a partir de ModuloPublicado.

- **Tablas DynamoDB** (PK `tenant_id`, solo `Query`): `sp_tarjetas`, `sp_intentos`
- **Publica:** QuizCompletado
- **Consume:** ModuloPublicado (cola sp-practica + DLQ)
- **Seguridad:** Lambda Authorizer de `ms-auth` en todas las rutas; el tenant sale del token, nunca del cliente.

## Endpoints
| Método | Ruta | Acceso | Handler |
|---|---|---|---|
| GET | `/practica/quiz` | token | `handler.obtener_quiz` |
| POST | `/practica/quiz/completar` | token | `handler.completar_quiz` |

## Desplegar
```bash
# con credenciales del Learner Lab vigentes y tras desplegar infra-base
npx --yes serverless@3 deploy --stage dev
```
Orden completo y atajo: `../docs-ops/deploy-all.sh`.

## Probar sin AWS
```bash
python3 ../pruebas-locales/test_e2e.py     # emulador con los serverless.yml reales
```
> `common.py` es **idéntico** en todos los servicios (la prueba lo verifica). Si lo cambias, cópialo a los demás.
