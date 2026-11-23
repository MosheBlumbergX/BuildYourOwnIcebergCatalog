# Build your own Iceberg REST catalogue

A read-only Iceberg REST catalogue in one file (`catalog.py`, about 100 lines of code). It runs locally,
needs no cloud account, and works with the standard PyIceberg client.

## What a catalogue is

A catalogue does one job: **map a table name to the location of its current metadata file.**
Schema, snapshots and data-file lists all live in that metadata file, which the client reads
itself. So a minimal catalogue is:

```
GET /v1/namespaces/demo/tables/trades
  -> { "metadata-location": "file:///.../metadata/00002-....metadata.json", "metadata": {...} }
```

`catalog.py` finds that location by scanning a warehouse folder for the highest-numbered
`*.metadata.json` in each table. There is no database.

## Files

| File | Role |
|---|---|
| `writer.py` | Stand-in for whatever produces your tables (Tableflow, Spark, Flink). Writes Iceberg files into `./warehouse`. |
| `catalog.py` | The catalogue. Reads `./warehouse`, serves the REST API, never writes. |
| `reader.py` | A standard PyIceberg REST client. It has no idea how the catalogue works. |

## Run it

Needs Python 3.11 to 3.14 (`pyarrow` has no wheels for newer versions yet).

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

python writer.py init                                   # 1,000 rows, 1 snapshot
uvicorn catalog:app --host 127.0.0.1 --port 8181 &      # start the catalogue
python reader.py                                        # read through it
```

Now add data and read again, without restarting the catalogue:

```bash
python writer.py append                                 # +500 rows, a new metadata file
python reader.py                                        # 1,500 rows, 2 snapshots
```

The catalogue never restarted or registered anything. It follows the files.

## Call it with curl

```bash
U=http://127.0.0.1:8181
TOK=$(curl -s -X POST $U/v1/oauth/tokens -d grant_type=client_credentials \
        -d client_id=demo-client -d client_secret=demo-secret | jq -r .access_token)
H="Authorization: Bearer $TOK"

curl -s -H "$H" $U/v1/namespaces
curl -s -H "$H" $U/v1/namespaces/demo/tables
curl -s -H "$H" $U/v1/namespaces/demo/tables/trades | jq -r '."metadata-location"'   # the pointer
curl -s -o /dev/null -w "%{http_code}\n" $U/v1/namespaces                            # 401, no token
curl -s -o /dev/null -w "%{http_code}\n" -X POST -H "$H" -d '{}' $U/v1/namespaces/demo/tables   # 405, read-only
```

## The endpoints

| Endpoint | Purpose |
|---|---|
| `POST /v1/oauth/tokens` | OAuth2 client credentials, returns a signed bearer token |
| `GET /v1/config` | Client bootstrap |
| `GET /v1/namespaces`, `GET /v1/namespaces/{ns}` | Namespaces |
| `GET /v1/namespaces/{ns}/tables` | Tables |
| `GET /v1/namespaces/{ns}/tables/{table}` | **Load table**: the location of the current metadata file |

Credentials default to `demo-client` / `demo-secret`. Override with `CATALOG_CLIENT_ID` and
`CATALOG_CLIENT_SECRET`, and point at another folder with `WAREHOUSE`.

## What it leaves out, on purpose

- **Writes.** No create, commit or drop. The producer owns table state; a second writer would
  corrupt it. A writable catalogue needs atomic compare-and-swap commits, which is the hard part.
- Multi-level namespaces, views, pagination, catalogue prefixes, `HEAD` endpoints, credential vending.
- Persistence and caching. It rescans the folder on every request, fine for a demo.
- Real storage. Locations are `file://`. On cloud storage the pointer would be `abfss://`, `s3://`
  or `gs://`, and the catalogue would list blobs instead of files.

If you need writes or production features, run Apache Polaris, Lakekeeper or Nessie rather than
extending this.

## Pointing a real engine at it

Anything that speaks the Iceberg REST spec can use it. Snowflake, for example, needs a catalogue
integration with `CATALOG_SOURCE = ICEBERG_REST`, an HTTPS endpoint it can reach, and an external
volume for the data. That means a public TLS endpoint and OAuth, so this local demo stops here.
