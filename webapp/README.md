# Private webapp (Module 7)

A minimal, clone-and-adapt example of the course's webapp pattern: render your
database into a fast static page, gate it behind a cookie in nginx, overlay live
prices, and add a trade button that only *proposes* (never places) orders.

This is a teaching starting point, not a finished dashboard — grow it by adding
queries to `queries.sql` and markers to `template.html`.

## Files

- `queries.sql` — one SQL block per `-- @@name` section, run read-only.
- `template.html` — the page layout; `@@name` markers are where results land.
- `build.py` — runs the queries as `claude_ro`, fills the template, writes `site/index.html`.
- `nginx-site.conf` — the cookie gate, TLS, and server-side token injection.

## Build it

```
pip install psycopg2-binary
RO_PASSWORD=your-readonly-password python3 build.py
```

That writes `site/index.html`. Re-run it after your daily data loads so the page
stays current (Module 3's schedule).

## Serve it

Point nginx at the `site/` folder using `nginx-site.conf` (adjust the domain,
cookie value, tokens and paths). Internal services — the price streamer on
`127.0.0.1:9100` and n8n on `127.0.0.1:5678` — stay bound to localhost; nginx is
the only front door and injects their tokens server-side.

## Safety

- `build.py` connects with the **read-only** role only. Rendering is a read.
- Tokens live in `nginx-site.conf` on the server, never in the page.
- The trade button POSTs to the n8n approval path — it never calls the broker
  directly, and the `trading_enabled` kill switch (Module 6) still applies.
