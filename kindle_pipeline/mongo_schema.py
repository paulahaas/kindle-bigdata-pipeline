"""Criação das coleções no MongoDB com validadores $jsonSchema e índices.

Coleções:
  books           agregado principal (preço, avaliação, selos, publicação e categoria embutidos)
  categories      estatísticas pré-calculadas por categoria
  authors         estatísticas pré-calculadas por autor
  ingestion_runs  log de auditoria de cada execução
"""
from pymongo import ASCENDING, DESCENDING, TEXT
from pymongo.database import Database

NUM = ["double", "int", "long", "decimal"]
NUM_OR_NULL = NUM + ["null"]

BOOKS_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["_id", "asin", "title", "author", "soldBy", "category", "pricing",
                     "rating", "badges", "publication"],
        "properties": {
            "_id": {"bsonType": "string"},
            "asin": {"bsonType": "string", "minLength": 1},
            "title": {"bsonType": "string"},
            "soldBy": {"bsonType": "string"},
            "imgUrl": {"bsonType": ["string", "null"]},
            "productUrl": {"bsonType": ["string", "null"]},
            "author": {
                "bsonType": "object",
                "required": ["name", "authorKey"],
                "properties": {
                    "name": {"bsonType": "string"},
                    "authorKey": {"bsonType": "string"},
                },
            },
            "category": {
                "bsonType": "object",
                "required": ["name"],
                "properties": {
                    "id": {"bsonType": ["int", "null"]},
                    "name": {"bsonType": "string"},
                },
            },
            "pricing": {
                "bsonType": "object",
                "required": ["priceRange", "isFree"],
                "properties": {
                    "price": {"bsonType": NUM_OR_NULL, "minimum": 0},
                    "priceRange": {"bsonType": "string"},
                    "isFree": {"bsonType": "bool"},
                },
            },
            "rating": {
                "bsonType": "object",
                "required": ["stars", "reviews", "hasReviews"],
                "properties": {
                    "stars": {"bsonType": NUM, "minimum": 0, "maximum": 5},
                    "reviews": {"bsonType": ["int", "long"], "minimum": 0},
                    "hasReviews": {"bsonType": "bool"},
                },
            },
            "badges": {
                "bsonType": "object",
                "required": ["isKindleUnlimited", "isBestSeller", "isEditorsPick", "isGoodReadsChoice"],
                "properties": {
                    "isKindleUnlimited": {"bsonType": "bool"},
                    "isBestSeller": {"bsonType": "bool"},
                    "isEditorsPick": {"bsonType": "bool"},
                    "isGoodReadsChoice": {"bsonType": "bool"},
                },
            },
            "publication": {
                "bsonType": "object",
                "properties": {
                    "date": {"bsonType": ["date", "null"]},
                    "year": {"bsonType": ["int", "null"]},
                },
            },
        },
    }
}

CATEGORIES_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["_id", "totalBooks", "totalReviews", "pctKindleUnlimited", "pctBestSeller"],
        "properties": {
            "_id": {"bsonType": "string", "description": "nome da categoria"},
            "categoryId": {"bsonType": ["int", "null"]},
            "totalBooks": {"bsonType": ["int", "long"], "minimum": 0},
            "totalReviews": {"bsonType": ["int", "long"], "minimum": 0},
            "avgStars": {"bsonType": NUM_OR_NULL},
            "avgReviews": {"bsonType": NUM_OR_NULL},
            "avgPrice": {"bsonType": NUM_OR_NULL},
            "pctKindleUnlimited": {"bsonType": NUM, "minimum": 0, "maximum": 100},
            "pctBestSeller": {"bsonType": NUM, "minimum": 0, "maximum": 100},
            "pctEditorsPick": {"bsonType": NUM, "minimum": 0, "maximum": 100},
            "pctGoodReadsChoice": {"bsonType": NUM, "minimum": 0, "maximum": 100},
            "updatedAt": {"bsonType": "date"},
        },
    }
}

AUTHORS_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["_id", "name", "totalBooks", "totalReviews"],
        "properties": {
            "_id": {"bsonType": "string", "description": "authorKey"},
            "name": {"bsonType": "string"},
            "totalBooks": {"bsonType": ["int", "long"], "minimum": 1},
            "totalReviews": {"bsonType": ["int", "long"], "minimum": 0},
            "avgStars": {"bsonType": NUM_OR_NULL},
            "avgPrice": {"bsonType": NUM_OR_NULL},
            "bestSellerCount": {"bsonType": ["int", "long"]},
            "kindleUnlimitedCount": {"bsonType": ["int", "long"]},
            "categories": {"bsonType": "array", "items": {"bsonType": "string"}},
            "firstYear": {"bsonType": ["int", "null"]},
            "lastYear": {"bsonType": ["int", "null"]},
            "updatedAt": {"bsonType": "date"},
        },
    }
}

RUNS_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["_id", "startedAt", "status", "source"],
        "properties": {
            "_id": {"bsonType": "string"},
            "status": {"enum": ["running", "success", "failed"]},
            "source": {"bsonType": "string"},
            "startedAt": {"bsonType": "date"},
            "finishedAt": {"bsonType": ["date", "null"]},
            "durationSeconds": {"bsonType": NUM_OR_NULL},
            "rowsRead": {"bsonType": ["int", "long"]},
            "rowsAccepted": {"bsonType": ["int", "long"]},
            "rowsRejected": {"bsonType": ["int", "long"]},
            "duplicatesRemoved": {"bsonType": ["int", "long"]},
            "booksUpserted": {"bsonType": ["int", "long"]},
            "rejectedByReason": {"bsonType": "object"},
            "error": {"bsonType": ["string", "null"]},
        },
    }
}

VALIDATORS = {
    "books": BOOKS_VALIDATOR,
    "categories": CATEGORIES_VALIDATOR,
    "authors": AUTHORS_VALIDATOR,
    "ingestion_runs": RUNS_VALIDATOR,
}


def ensure_collections(db: Database) -> None:
    existing = set(db.list_collection_names())
    for name, validator in VALIDATORS.items():
        if name in existing:
            db.command("collMod", name, validator=validator,
                       validationLevel="strict", validationAction="error")
        else:
            db.create_collection(name, validator=validator,
                                 validationLevel="strict", validationAction="error")
    print(f"[mongo] Coleções/validadores prontos: {', '.join(VALIDATORS)}")


def ensure_indexes(db: Database) -> None:
    b = db.books
    b.create_index([("asin", ASCENDING)], unique=True, name="ux_asin")
    b.create_index([("category.name", ASCENDING), ("rating.stars", DESCENDING)], name="ix_category_stars")
    b.create_index([("author.authorKey", ASCENDING)], name="ix_author")
    b.create_index([("publication.year", ASCENDING), ("category.name", ASCENDING)], name="ix_year_category")
    b.create_index([("pricing.priceRange", ASCENDING)], name="ix_price_range")
    b.create_index([("badges.isKindleUnlimited", ASCENDING)], name="ix_ku")
    b.create_index([("badges.isBestSeller", ASCENDING)], name="ix_bestseller")
    b.create_index([("rating.reviews", DESCENDING)], name="ix_reviews")
    b.create_index([("title", TEXT)], name="tx_title", default_language="english")

    db.categories.create_index([("avgStars", DESCENDING)], name="ix_avg_stars")
    db.categories.create_index([("totalBooks", DESCENDING)], name="ix_total_books")

    db.authors.create_index([("totalReviews", DESCENDING)], name="ix_total_reviews")
    db.authors.create_index([("avgStars", DESCENDING), ("totalReviews", DESCENDING)], name="ix_ranking")

    db.ingestion_runs.create_index([("startedAt", DESCENDING)], name="ix_started")
    print("[mongo] Índices criados")
