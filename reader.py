"""Read the table through the REST catalogue with the standard PyIceberg client.

Nothing here knows how the catalogue works: it is the same code you would point at
Tableflow's catalogue, Polaris or Unity. Only the URI and credential change.

    python reader.py
"""
import os

from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog(
    "demo",
    uri=os.environ.get("CATALOG_URI", "http://127.0.0.1:8181"),
    credential=f"{os.environ.get('CATALOG_CLIENT_ID', 'demo-client')}:{os.environ.get('CATALOG_CLIENT_SECRET', 'demo-secret')}",
)

print("namespaces:", catalog.list_namespaces())
print("tables:    ", catalog.list_tables("demo"))

table = catalog.load_table("demo.trades")
data = table.scan().to_arrow()
print(f"\ndemo.trades: {data.num_rows} rows, {len(table.snapshots())} snapshot(s), current {table.current_snapshot().snapshot_id}")
print(data.slice(0, 3).to_pandas().to_string(index=False))
