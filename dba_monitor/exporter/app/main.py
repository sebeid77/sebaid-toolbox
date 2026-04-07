import logging

from fastapi import FastAPI, Response
from prometheus_client import CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST

from .config import load_databases
from .logging_conf import setup_logging
from .collector import AuroraPostgresCollector

setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI(title="Aurora Postgres Exporter")


def build_collectors():
    dbs = load_databases()
    return [AuroraPostgresCollector(db) for db in dbs]


@app.get("/metrics")
async def metrics() -> Response:
    registry = CollectorRegistry()
    collectors = build_collectors()

    for c in collectors:
        try:
            c.collect_metrics(registry)
        except Exception as exc:
            logger.exception("Collector failed for %s: %s", c.name, exc)

    output = generate_latest(registry)
    return Response(content=output, media_type=CONTENT_TYPE_LATEST)


@app.get("/")
async def root():
    return {"status": "ok", "message": "Aurora Postgres exporter"}
