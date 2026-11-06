#!/usr/bin/env python3
"""
Build the private webapp (Module 7).

Runs each query in queries.sql with the READ-ONLY database role, drops the
results into template.html at the matching @@marker, and writes site/index.html.
Rendering is a read: this connects as claude_ro, never the owner.

Usage:
    RO_PASSWORD=your-readonly-password python3 build.py
"""
import os
import html
import psycopg2

DB = dict(
    host="127.0.0.1",
    port=5432,
    dbname="market_data",
    user="claude_ro",
    password=os.environ["RO_PASSWORD"],   # read-only role only
)


def load_queries(path="queries.sql"):
    """Split queries.sql into {name: sql} on '-- @@name' section headers."""
    queries, name, buf = {}, None, []
    for line in open(path).read().splitlines():
        if line.strip().startswith("-- @@"):
            if name:
                queries[name] = "\n".join(buf).strip()
            name, buf = line.strip()[5:].strip(), []
        else:
            buf.append(line)
    if name:
        queries[name] = "\n".join(buf).strip()
    return queries


def fetch(cur, sql):
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def positions_table(rows):
    head = ("<tr><th>Ticker</th><th>Shares</th><th>Entry</th><th>Stop</th>"
            "<th>Target</th><th>Price</th><th>P&amp;L</th></tr>")
    body = ""
    for r in rows:
        tk = html.escape(str(r["ticker"]))
        body += (
            f"<tr data-symbol='{tk}' data-entry='{r['entry_price']}' "
            f"data-shares='{r['shares']}'>"
            f"<td>{tk}</td><td>{r['shares']}</td><td>{r['entry_price']}</td>"
            f"<td>{r['initial_stop']}</td><td>{r['target']}</td>"
            f"<td class='live-price'>&mdash;</td>"
            f"<td class='live-pnl'>&mdash;</td></tr>"
        )
    return f"<table>{head}{body}</table>"


def main():
    queries = load_queries()
    template = open("template.html").read()
    with psycopg2.connect(**DB) as conn, conn.cursor() as cur:
        positions = fetch(cur, queries["positions"])

    out = template.replace("@@positions", positions_table(positions))
    os.makedirs("site", exist_ok=True)
    with open("site/index.html", "w") as f:
        f.write(out)
    print(f"wrote site/index.html with {len(positions)} open positions")


if __name__ == "__main__":
    main()
