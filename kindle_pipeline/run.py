"""Orquestrador do pipeline completo.

Uso:
  python -m kindle_pipeline.run                 # baixa do Kaggle (se preciso) e executa tudo
  python -m kindle_pipeline.run --sample        # usa a amostra sintética de data/sample/
  python -m kindle_pipeline.run --input arq.csv # usa um CSV específico
  python -m kindle_pipeline.run --analyses      # executa também as análises e o modelo
"""
import argparse
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

from . import config


def parse_args():
    p = argparse.ArgumentParser(description="Pipeline Amazon Kindle Books -> MongoDB")
    src = p.add_mutually_exclusive_group()
    src.add_argument("--sample", action="store_true", help="usar a amostra sintética (gera se não existir)")
    src.add_argument("--input", type=Path, help="caminho de um CSV já baixado")
    p.add_argument("--analyses", action="store_true", help="rodar análises e modelo ao final")
    return p.parse_args()


def resolve_source(args) -> Path:
    if args.input:
        return args.input.resolve()
    if args.sample:
        if not config.SAMPLE_FILE.exists():
            from .make_sample import generate
            generate(config.SAMPLE_FILE)
        return config.SAMPLE_FILE
    from .download import download_dataset
    return download_dataset()


def main():
    args = parse_args()
    source = resolve_source(args)

    # imports pesados só depois de resolver a fonte
    from .mongo_load import build_aggregates, get_db, load_books
    from .mongo_schema import ensure_collections, ensure_indexes
    from .spark_ingest import build_spark, run as spark_run

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    started = datetime.now(timezone.utc)
    t0 = time.perf_counter()

    db = get_db()
    ensure_collections(db)
    db.ingestion_runs.insert_one({
        "_id": run_id, "status": "running", "source": str(source), "startedAt": started,
    })
    print(f"\n=== Execução {run_id} | fonte: {source} ===\n")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        metrics = spark_run(spark, source, run_id)
        upserted = load_books(spark, db, run_id)
        aggregates = build_aggregates(db)
        ensure_indexes(db)

        duration = round(time.perf_counter() - t0, 2)
        db.ingestion_runs.update_one({"_id": run_id}, {"$set": {
            "status": "success",
            "finishedAt": datetime.now(timezone.utc),
            "durationSeconds": duration,
            "rowsRead": metrics["rowsRead"],
            "rowsAccepted": metrics["rowsAccepted"],
            "rowsRejected": metrics["rowsRejected"],
            "duplicatesRemoved": metrics["duplicatesRemoved"],
            "rejectedByReason": metrics["rejectedByReason"],
            "booksUpserted": upserted,
            "categoriesBuilt": aggregates["categories"],
            "authorsBuilt": aggregates["authors"],
            "rejectedPath": metrics["rejectedPath"],
            "curatedPath": metrics["curatedPath"],
        }})

        print("\n=== Resumo da execução ===")
        print(f"  run_id              : {run_id}")
        print(f"  linhas lidas        : {metrics['rowsRead']:,}")
        print(f"  rejeitadas          : {metrics['rowsRejected']:,} {metrics['rejectedByReason']}")
        print(f"  duplicadas removidas: {metrics['duplicatesRemoved']:,}")
        print(f"  aceitas (books)     : {metrics['rowsAccepted']:,}")
        print(f"  categorias / autores: {aggregates['categories']:,} / {aggregates['authors']:,}")
        print(f"  duração             : {duration}s\n")
    except Exception as exc:
        db.ingestion_runs.update_one({"_id": run_id}, {"$set": {
            "status": "failed",
            "finishedAt": datetime.now(timezone.utc),
            "durationSeconds": round(time.perf_counter() - t0, 2),
            "error": f"{type(exc).__name__}: {exc}",
        }})
        traceback.print_exc()
        raise SystemExit(1)
    finally:
        spark.stop()

    if args.analyses:
        from .analyses import run_all
        run_all(db)
        from .model import train
        train(db)


if __name__ == "__main__":
    main()
