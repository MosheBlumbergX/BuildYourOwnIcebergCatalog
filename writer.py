"""Stand-in for whatever produces your Iceberg tables (Tableflow, Spark, Flink...).

It writes Iceberg data and metadata files into ./warehouse. It keeps its own private
bookkeeping in ./.writer, which the catalogue never looks at: the catalogue only
needs the files, which is the point of the demo.

    python writer.py init        create demo.trades with 1,000 rows
    python writer.py append      add 500 rows (a new snapshot and a new metadata file)
"""
import random
import sys
import time
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

ROOT = Path(__file__).parent.resolve()
WAREHOUSE = ROOT / "warehouse"
IDENTIFIER = ("demo", "trades")


def rows(n):
    return pa.table({
        "symbol": [random.choice(["ZVZZT", "ZWZZT", "ZXZZT", "ZJZZT"]) for _ in range(n)],
        "side": [random.choice(["BUY", "SELL"]) for _ in range(n)],
        "quantity": [random.randint(1, 5000) for _ in range(n)],
        "price": [round(random.uniform(5, 1000), 2) for _ in range(n)],
        "trade_ts": [int(time.time() * 1000) for _ in range(n)],
    })


def main(command):
    WAREHOUSE.mkdir(exist_ok=True)
    (ROOT / ".writer").mkdir(exist_ok=True)
    catalog = SqlCatalog("writer", uri=f"sqlite:///{ROOT}/.writer/catalog.db", warehouse=WAREHOUSE.as_uri())
    catalog.create_namespace_if_not_exists(IDENTIFIER[0])

    if command == "init":
        first = rows(1000)
        table = catalog.create_table_if_not_exists(IDENTIFIER, schema=first.schema)
        if table.current_snapshot() is None:
            table.append(first)
    elif command == "append":
        table = catalog.load_table(IDENTIFIER)
        table.append(rows(500))
    else:
        raise SystemExit(__doc__)

    table = catalog.load_table(IDENTIFIER)
    print(f"{'.'.join(IDENTIFIER)}: {len(table.snapshots())} snapshot(s), current {table.current_snapshot().snapshot_id}")
    print(f"metadata: {Path(table.metadata_location.removeprefix('file://')).name}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "")
