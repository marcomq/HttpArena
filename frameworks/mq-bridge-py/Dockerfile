# HttpArena image for the mq-bridge-py (Python) entry.
#
# Builds the mq_bridge_py wheel with maturin (http feature only) and runs
# server.py on port 8080. Build and runtime share the same Python base image so
# the compiled extension ABI matches. Pin MQB_REF to a released tag.
FROM python:3.12-slim AS build

ARG MQB_REF=v0.2.21
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl build-essential git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"
RUN pip install --no-cache-dir maturin

RUN git clone --depth 1 -b "${MQB_REF}" https://github.com/marcomq/mq-bridge /src
WORKDIR /src/python/mq-bridge-py
RUN maturin build --release --no-default-features -F http -F rustls-ring -F pyo3/extension-module -o /wheels

FROM python:3.12-slim
RUN groupadd --system appuser \
    && useradd --system --gid appuser --create-home --home-dir /home/appuser --shell /usr/sbin/nologin appuser \
    && mkdir -p /app /wheels \
    && chown -R appuser:appuser /app /wheels
COPY --from=build --chown=appuser:appuser /wheels/*.whl /wheels/
# psycopg[binary] powers the optional /async-db profile; absent DATABASE_URL it is unused.
RUN pip install --no-cache-dir /wheels/*.whl "psycopg[binary]>=3.1" "psycopg_pool>=3.2"
WORKDIR /app
COPY --chown=appuser:appuser server.py /app/server.py
EXPOSE 8080
# Take CPython's cyclic GC off the request hot path: the JSON handlers allocate
# many short-lived dicts/lists per request (no reference cycles), so disabling
# the periodic collector removes its scan overhead with no leak. `count` is the
# safe alternative if a handler ever introduces cycles.
ENV MQ_BRIDGE_PY_GC_MODE=off
# Scale Python across cores with one process per core (single GIL each),
# co-binding port 8080 via SO_REUSEPORT. Unset/0 => all cores; set 1 to disable.
ENV MQB_WORKERS=0
USER appuser:appuser
CMD ["python", "server.py"]
