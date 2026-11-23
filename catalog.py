"""A read-only Iceberg REST catalogue in one file.

An Iceberg catalogue does one job: map a table name to the location of its
current metadata file. Everything else (schema, snapshots, data files) lives in
that metadata file, which the client reads itself.

This one finds tables by scanning a warehouse folder:

    warehouse/<namespace>/<table>/metadata/00003-<uuid>.metadata.json

The highest-numbered metadata file is the current version. It serves just enough
of the Iceberg REST spec for a client to list and load tables:

    POST /v1/oauth/tokens                           client credentials -> bearer token
    GET  /v1/config                                 client bootstrap
    GET  /v1/namespaces                             list namespaces
    GET  /v1/namespaces/{ns}                        namespace properties
    GET  /v1/namespaces/{ns}/tables                 list tables
    GET  /v1/namespaces/{ns}/tables/{table}         load table (the important one)

There are no write endpoints, so clients can read but not create or commit. Whatever
produces the Iceberg files (Tableflow, Spark, Flink...) owns the table state.
"""
import hashlib
import hmac
import json
import os
import re
import time
from pathlib import Path
from urllib.parse import parse_qs

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

WAREHOUSE = Path(os.environ.get("WAREHOUSE", "warehouse")).resolve()
CLIENT_ID = os.environ.get("CATALOG_CLIENT_ID", "demo-client")
CLIENT_SECRET = os.environ.get("CATALOG_CLIENT_SECRET", "demo-secret")
TOKEN_TTL_SECONDS = 3600

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)


class CatalogError(Exception):
    def __init__(self, status, kind, message):
        self.status, self.kind, self.message = status, kind, message


@app.exception_handler(CatalogError)
def on_error(_request, exc):
    return JSONResponse({"error": {"message": exc.message, "type": exc.kind, "code": exc.status}}, status_code=exc.status)


# --------------------------------------------------------------------------
# 1. Finding tables: the whole "catalogue" is this function
# --------------------------------------------------------------------------
METADATA_FILE = re.compile(r"^(\d+)-.*\.metadata\.json$")


def scan():
    """{(namespace, table): path of the newest metadata file}"""
    tables = {}
    for metadata_dir in WAREHOUSE.glob("*/*/metadata"):
        versions = [(int(m.group(1)), f) for f in metadata_dir.iterdir() if (m := METADATA_FILE.match(f.name))]
        if versions:
            namespace = metadata_dir.parent.parent.name.removesuffix(".db")  # some writers add ".db"
            tables[(namespace, metadata_dir.parent.name)] = max(versions)[1]
    return tables


# --------------------------------------------------------------------------
# 2. Auth: OAuth2 client credentials, with a stateless signed token
# --------------------------------------------------------------------------
def sign(payload):
    return hmac.new(CLIENT_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()


def require_token(request):
    scheme, _, token = request.headers.get("authorization", "").partition(" ")
    expires, _, signature = token.partition(".")
    if not (scheme.lower() == "bearer" and expires.isdigit() and hmac.compare_digest(signature, sign(expires)) and int(expires) > time.time()):
        raise CatalogError(401, "NotAuthorizedException", "missing or invalid bearer token")


@app.post("/v1/oauth/tokens")
async def token(request: Request):
    form = {k: v[0] for k, v in parse_qs((await request.body()).decode()).items()}
    if form.get("grant_type") != "client_credentials":
        raise CatalogError(400, "OAuthError", "unsupported_grant_type")
    if not (hmac.compare_digest(form.get("client_id", ""), CLIENT_ID) and hmac.compare_digest(form.get("client_secret", ""), CLIENT_SECRET)):
        return JSONResponse({"error": "invalid_client"}, status_code=401)
    expires = str(int(time.time()) + TOKEN_TTL_SECONDS)
    return {"access_token": f"{expires}.{sign(expires)}", "token_type": "bearer", "expires_in": TOKEN_TTL_SECONDS}


# --------------------------------------------------------------------------
# 3. The Iceberg REST endpoints
# --------------------------------------------------------------------------
def namespaces():
    return sorted({ns for ns, _ in scan()})


def require_namespace(namespace):
    if namespace not in namespaces():
        raise CatalogError(404, "NoSuchNamespaceException", f"namespace {namespace} does not exist")


@app.get("/v1/config")
def config(request: Request):
    require_token(request)
    return {"defaults": {}, "overrides": {}}


@app.get("/v1/namespaces")
def list_namespaces(request: Request):
    require_token(request)
    return {"namespaces": [[ns] for ns in namespaces()]}


@app.get("/v1/namespaces/{namespace}")
def get_namespace(namespace: str, request: Request):
    require_token(request)
    require_namespace(namespace)
    return {"namespace": [namespace], "properties": {}}


@app.get("/v1/namespaces/{namespace}/tables")
def list_tables(namespace: str, request: Request):
    require_token(request)
    require_namespace(namespace)
    return {"identifiers": [{"namespace": [ns], "name": t} for ns, t in sorted(scan()) if ns == namespace]}


@app.get("/v1/namespaces/{namespace}/tables/{table}")
def load_table(namespace: str, table: str, request: Request):
    require_token(request)
    require_namespace(namespace)
    path = scan().get((namespace, table))
    if path is None:
        raise CatalogError(404, "NoSuchTableException", f"table {namespace}.{table} does not exist")
    return {
        "metadata-location": path.as_uri(),   # the pointer: this is what a catalogue is for
        "metadata": json.loads(path.read_text()),
        "config": {},
    }
