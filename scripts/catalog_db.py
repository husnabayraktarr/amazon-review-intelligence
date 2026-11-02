"""
Product catalog database (RDS PostgreSQL) - the "operational" source for CDC.

Usage:
  python scripts/catalog_db.py check
  python scripts/catalog_db.py seed data/products_seed.csv
  python scripts/catalog_db.py change --updates 300 --inserts 3 --deletes 2

Env vars: PGHOST (RDS endpoint), PGPASSWORD
"""
import argparse
import csv
import os
import random
import string

import psycopg

DDL = """
CREATE TABLE IF NOT EXISTS products (
    parent_asin   VARCHAR(20) PRIMARY KEY,
    title         TEXT,
    brand         TEXT,
    main_category TEXT,
    price         NUMERIC(10,2),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def connect():
    return psycopg.connect(
        host=os.environ["PGHOST"], port=5432, dbname="productdb",
        user="catalog_admin", password=os.environ["PGPASSWORD"],
        sslmode="require", connect_timeout=10,
    )


def check():
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SHOW wal_level")
        print(f"[check] wal_level={cur.fetchone()[0]}  (must be 'logical' for CDC)")
        cur.execute("SELECT to_regclass('public.products')")
        if cur.fetchone()[0]:
            cur.execute("SELECT COUNT(*), COUNT(price), MAX(updated_at) FROM products")
            total, priced, last = cur.fetchone()
            print(f"[check] products={total} with_price={priced} last_update={last}")
        else:
            print("[check] products table not created yet")


def seed(path):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute("TRUNCATE products")
        n = 0
        with open(path, newline="", encoding="utf-8") as f, \
             cur.copy("COPY products (parent_asin, title, brand, main_category, price) FROM STDIN") as copy:
            for row in csv.DictReader(f):
                copy.write_row((
                    row["parent_asin"],
                    row["title"] or None,
                    row["brand"] or None,
                    row["main_category"] or None,
                    row["price"] or None,
                ))
                n += 1
        conn.commit()
        cur.execute("SELECT COUNT(*), COUNT(price) FROM products")
        total, priced = cur.fetchone()
        print(f"[seed] read={n} rows_in_table={total} with_price={priced}")


def change(updates, inserts, deletes):
    """Simulate real business activity so DMS has inserts, updates and deletes to capture."""
    rng = random.Random()
    with connect() as conn, conn.cursor() as cur:
        # Price changes: discounts and increases
        cur.execute("SELECT parent_asin, price FROM products WHERE price IS NOT NULL "
                    "ORDER BY random() LIMIT %s", (updates,))
        for asin, price in cur.fetchall():
            factor = rng.choice([0.70, 0.80, 0.90, 1.10, 1.25])
            cur.execute("UPDATE products SET price = %s, updated_at = now() WHERE parent_asin = %s",
                        (round(float(price) * factor, 2), asin))

        # New products launched
        for _ in range(inserts):
            asin = "BNEW" + "".join(rng.choices(string.ascii_uppercase + string.digits, k=6))
            cur.execute("INSERT INTO products (parent_asin, title, brand, main_category, price) "
                        "VALUES (%s, %s, %s, %s, %s)",
                        (asin, "New demo appliance " + asin, "DemoBrand", "Appliances",
                         round(rng.uniform(20, 400), 2)))

        # Discontinued products
        cur.execute("SELECT parent_asin FROM products WHERE parent_asin NOT LIKE 'BNEW%%' "
                    "ORDER BY random() LIMIT %s", (deletes,))
        for (asin,) in cur.fetchall():
            cur.execute("DELETE FROM products WHERE parent_asin = %s", (asin,))

        conn.commit()
    print(f"[change] updated={updates} inserted={inserts} deleted={deletes}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check")
    s = sub.add_parser("seed")
    s.add_argument("path")
    c = sub.add_parser("change")
    c.add_argument("--updates", type=int, default=300)
    c.add_argument("--inserts", type=int, default=3)
    c.add_argument("--deletes", type=int, default=2)
    args = parser.parse_args()

    if args.cmd == "check":
        check()
    elif args.cmd == "seed":
        seed(args.path)
    else:
        change(args.updates, args.inserts, args.deletes)
