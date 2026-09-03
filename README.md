# telemetry

The internet side of mayara telemetry data, so that we have an inkling of how many users there are.

A single Rust binary that receives the anonymous "it works" reports
[mayara](https://github.com/keesverruijt/mayara-server) sends, stores them in SQLite, and serves
both the raw reports and an aggregate view. It runs on `telemetry.keversoft.com` behind nginx,
which terminates TLS; the collector itself listens on loopback only.

## What is collected

Exactly what mayara sends, and nothing else. Per report: a random install id created on first run,
the mayara version, whether the build came from mayara's own CI or was compiled elsewhere, the
operating system and architecture, how mayara was launched, the radar brand and model, how many
radars were seen, whether the radar is dual range, how many hours the radar has transmitted over
its life where it counts that, and either the seconds until the first spoke arrived or the name of
the first control the user changed.

There is no position, serial number, network address or vessel data in a report, and none is added
here: **the address a report arrives from is never written to disk**. It is held in memory only, for
the length of one rate limit window, so a single sender cannot flood the collector.

Mayara sends at most two reports per run, and reporting is off with `--no-telemetry` or
`MAYARA_NO_TELEMETRY=1`.

## API

The collected data is anonymous and public: every `GET` answers with `Access-Control-Allow-Origin: *`.

Everything is served under a `/mayara/` prefix so the host can carry other projects later;
`https://telemetry.keversoft.com/` redirects there. The collector itself serves at its own root and
builds every URL in its page relative to the current one, so it neither knows nor cares which prefix
it is mounted under — nginx strips it.

| Endpoint | Purpose |
| --- | --- |
| `POST /mayara/v1/event` | Receive one report. `204` on success. |
| `GET /mayara/v1/stats?days=90` | Aggregate view. `days` is clamped to 1…3650, and defaults to 90. |
| `GET /mayara/v1/events?limit=100` | The most recent reports, newest first. `limit` is clamped to 1…1000. |
| `GET /mayara/health` | `{"status":"ok","last_event":…}`, or `503` if the database is unreachable. |
| `GET /mayara/` | The page that reads `v1/stats`. |

A report must be a JSON object of at most 20 KB carrying at least `install` and `event`; every
other field is optional but must have the type it is stored in. Fields this collector does not know
about are ignored, but the body is stored verbatim, so a report from a newer mayara loses nothing.
That is also how the database is brought forward: the columns a report is split into are named
after its fields, so teaching the collector a field adds the column on the next start and fills it
from the bodies already stored, and a field that falls out of use drops its column.

```console
$ curl -X POST https://telemetry.keversoft.com/mayara/v1/event \
    -H 'content-type: application/json' \
    -d '{"install":"…","event":"spokes","version":"3.10.0","os":"linux","brand":"Navico"}'
```

Refusals: `400` for a body that is not a usable JSON object, `413` over 20 KB, `429` when one
address reports more than 60 times an hour or one install more than 50 times a day.

Every breakdown in `/v1/stats` counts installs as well as reports, so one install that restarts
often cannot outweigh a brand many boats use.

## Running it

```console
$ cargo run -- --listen 127.0.0.1:8099 --database telemetry.db
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--listen` | `127.0.0.1:8099` | Address to listen on. Keep it on loopback. |
| `--database` | `telemetry.db` | SQLite file, created if it does not exist. |
| `--rate-limit` | `60` | Reports accepted per client address per hour. |

`RUST_LOG=debug` logs every accepted and refused report.

## Deploying

The collector runs as a container on keversoft.com's existing `docker_apps` network, alongside the
`webserver` (nginx) container that terminates TLS. Nothing is published on the host: nginx reaches
the collector by container name at `http://telemetry:8099` and mounts it under `/mayara/`.

```console
# git clone … /docker/telemetry && cd /docker/telemetry
# install -d -o 1000 -g 1000 /docker-volumes/telemetry
# docker compose -f docker/docker-compose.yml up -d --build
```

The database is the only state, at `/docker-volumes/telemetry/telemetry.db`. Back that up and
nothing else. It is written by uid 1000 inside the container, which is why the host directory has to
be owned by 1000 — a bind mount is not chowned for you.

Then the certificate and the site:

```console
# certbot certonly --webroot -d telemetry.keversoft.com
# cp deploy/telemetry.keversoft.com.conf /docker/nginx/sites-available/
# ln -s ../sites-available/telemetry.keversoft.com.conf /docker/nginx/sites-enabled/
# docker exec webserver nginx -t && docker exec webserver nginx -s reload
```

Issue the certificate first: nginx refuses to start if a site names one that is not there yet.

Three details in the site config are load-bearing. The `/mayara/` prefix is stripped with a
`rewrite … break` rather than a URI on `proxy_pass`, because nginx does not apply that form when the
upstream is named through a variable. `proxy_pass` goes through a variable so the
container name is looked up per request against Docker's embedded DNS — a literal name is resolved
once at startup, and nginx would keep proxying to a stale address after the collector container is
recreated. And `X-Forwarded-For` carries the peer nginx saw as its last entry, which is the entry
the collector believes and rate limits on; a collector reachable without going through nginx would
take a sender's word for its own address.

To keep everything in the fleet's `/docker/docker-compose.yml` instead, paste the `telemetry:`
service block there with `apps` as its network and `context: /docker/telemetry` as its build.
Watchtower does not need to know about it: the image is built here, not pulled.

Mayara only reports once it has a collector to report to: `DEFAULT_ENDPOINT` in
`src/lib/telemetry.rs` has to be set to `https://telemetry.keversoft.com/mayara/v1/event` before
any release starts sending.

## Layout

| File | Contents |
| --- | --- |
| `src/event.rs` | What a report must look like to be accepted |
| `src/db.rs` | Schema, storage, the per-install cap, and bringing an older database forward |
| `src/stats.rs` | The aggregate queries behind `/v1/stats` |
| `src/ratelimit.rs` | The per-address budget, in memory only |
| `src/web.rs` | Routes and handlers |
| `static/index.html` | The page, embedded into the binary at build time |
| `docker/` | Image and compose file |
| `deploy/telemetry.keversoft.com.conf` | nginx site for the webserver container |
