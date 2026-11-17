"""Etapas 6 e 7: carga no MongoDB (upsert por asin) e construção das coleções agregadas."""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import TYPE_CHECKING

from pymongo import MongoClient, ReplaceOne
from pymongo.database import Database

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

from . import config


def get_db() -> Database:
    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=10_000, tz_aware=True)
    client.admin.command("ping")
    return client[config.MONGO_DB]


def _to_datetime(d):
    if d is None:
        return None
    if isinstance(d, datetime):
        return d
    if isinstance(d, date):
        return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return None


def row_to_document(r: dict, run_id: str, loaded_at: datetime) -> dict:
    """Converte uma linha da camada curated no documento do agregado `books`."""
    return {
        "_id": r["asin"],
        "asin": r["asin"],
        "title": r["title"],
        "author": {"name": r["author_name"], "authorKey": r["author_key"]},
        "soldBy": r["sold_by"],
        "imgUrl": r["img_url"],
        "productUrl": r["product_url"],
        "category": {"id": r["category_id"], "name": r["category_name"]},
        "pricing": {"price": r["price"], "priceRange": r["price_range"], "isFree": bool(r["is_free"])},
        "rating": {"stars": float(r["stars"]), "reviews": int(r["reviews"]), "hasReviews": bool(r["has_reviews"])},
        "badges": {
            "isKindleUnlimited": bool(r["is_kindle_unlimited"]),
            "isBestSeller": bool(r["is_best_seller"]),
            "isEditorsPick": bool(r["is_editors_pick"]),
            "isGoodReadsChoice": bool(r["is_goodreads_choice"]),
        },
        "publication": {
            "date": _to_datetime(r["published_date"]),
            "year": int(r["publication_year"]) if r["publication_year"] is not None else None,
        },
        "ingestion": {"runId": run_id, "loadedAt": loaded_at},
    }


def load_books(spark: SparkSession, db: Database, run_id: str) -> int:
    df = spark.read.parquet(str(config.CURATED_DIR))
    loaded_at = datetime.now(timezone.utc)
    batch, total = [], 0

    for row in df.toLocalIterator():
        doc = row_to_document(row.asDict(), run_id, loaded_at)
        batch.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        if len(batch) >= config.LOAD_BATCH_SIZE:
            db.books.bulk_write(batch, ordered=False)
            total += len(batch)
            print(f"[mongo] {total:,} livros carregados...")
            batch = []
    if batch:
        db.books.bulk_write(batch, ordered=False)
        total += len(batch)

    # Remove livros que não vieram nesta carga (ex.: passaram a ser rejeitados)
    stale = db.books.delete_many({"ingestion.runId": {"$ne": run_id}}).deleted_count
    print(f"[mongo] books: {total:,} upserts, {stale:,} removidos por não estarem mais na fonte")
    return total


def _ratio(field):
    """Acumulador ($group): fração de documentos em que o campo booleano é true."""
    return {"$avg": {"$cond": [f"${field}", 1, 0]}}


def _to_pct(name):
    """Expressão ($set/$project): converte a fração em percentual 0-100 com 2 casas."""
    return {"$round": [{"$multiply": [f"${name}", 100]}, 2]}


PCT_FIELDS = ["pctKindleUnlimited", "pctBestSeller", "pctEditorsPick", "pctGoodReadsChoice"]


CATEGORIES_PIPELINE = [
    {"$group": {
        "_id": "$category.name",
        "categoryId": {"$first": "$category.id"},
        "totalBooks": {"$sum": 1},
        "totalReviews": {"$sum": "$rating.reviews"},
        # média de estrelas considera apenas livros que têm avaliações
        "avgStars": {"$avg": {"$cond": ["$rating.hasReviews", "$rating.stars", None]}},
        "avgReviews": {"$avg": "$rating.reviews"},
        "avgPrice": {"$avg": "$pricing.price"},
        "pctKindleUnlimited": _ratio("badges.isKindleUnlimited"),
        "pctBestSeller": _ratio("badges.isBestSeller"),
        "pctEditorsPick": _ratio("badges.isEditorsPick"),
        "pctGoodReadsChoice": _ratio("badges.isGoodReadsChoice"),
    }},
    {"$set": {
        "avgStars": {"$round": ["$avgStars", 2]},
        "avgReviews": {"$round": ["$avgReviews", 1]},
        "avgPrice": {"$round": ["$avgPrice", 2]},
        **{f: _to_pct(f) for f in PCT_FIELDS},
        "updatedAt": "$$NOW",
    }},
    {"$merge": {"into": "categories", "on": "_id", "whenMatched": "replace", "whenNotMatched": "insert"}},
]

AUTHORS_PIPELINE = [
    {"$group": {
        "_id": "$author.authorKey",
        "name": {"$first": "$author.name"},
        "totalBooks": {"$sum": 1},
        "totalReviews": {"$sum": "$rating.reviews"},
        "avgStars": {"$avg": {"$cond": ["$rating.hasReviews", "$rating.stars", None]}},
        "avgPrice": {"$avg": "$pricing.price"},
        "bestSellerCount": {"$sum": {"$cond": ["$badges.isBestSeller", 1, 0]}},
        "kindleUnlimitedCount": {"$sum": {"$cond": ["$badges.isKindleUnlimited", 1, 0]}},
        "categories": {"$addToSet": "$category.name"},
        "firstYear": {"$min": "$publication.year"},
        "lastYear": {"$max": "$publication.year"},
    }},
    {"$set": {
        "avgStars": {"$round": ["$avgStars", 2]},
        "avgPrice": {"$round": ["$avgPrice", 2]},
        "updatedAt": "$$NOW",
    }},
    {"$merge": {"into": "authors", "on": "_id", "whenMatched": "replace", "whenNotMatched": "insert"}},
]


def build_aggregates(db: Database) -> dict:
    db.categories.delete_many({})
    db.books.aggregate(CATEGORIES_PIPELINE, allowDiskUse=True)
    db.authors.delete_many({})
    db.books.aggregate(AUTHORS_PIPELINE, allowDiskUse=True)
    counts = {
        "categories": db.categories.count_documents({}),
        "authors": db.authors.count_documents({}),
    }
    print(f"[mongo] Agregados: {counts['categories']:,} categorias, {counts['authors']:,} autores")
    return counts
