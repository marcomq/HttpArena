# veb

[veb](https://modules.vlang.io/veb.html) is the web framework that ships with
the [V](https://vlang.io) standard library.

## Implemented tests

| Test | Endpoint |
|------|----------|
| `pipelined` | `GET /pipeline` |
| `json` | `GET /json/{count}?m=M` (over `/data/dataset.json`) |
| `async-db` | `GET /async-db?min&max&limit` via `db.pg` |

> The `baseline` profile is not subscribed: veb does not decode chunked request
> bodies, which baseline validation requires. `GET/POST /baseline11` are still
> implemented for non-chunked requests.

## Stack

* [V](https://vlang.io) — pinned master commit `c0624b274` (built from source)
* [veb](https://modules.vlang.io/veb.html) — HTTP framework, built with the default
  GC (Boehm; `-prealloc` is a never-free arena unsuited to a server)
* `db.pg` (stdlib) — pooled Go-style PostgreSQL driver (`db.exec_param_many`)

JSON is serialized manually (precomputed prefixes + `strings.Builder`) to avoid
per-request reflection.
