"""Etapas 2 a 5: leitura com Spark, validação, limpeza, transformação e escrita em Parquet.

Saídas:
  data/curated/books/publication_year=AAAA/*.parquet  -> registros válidos
  data/rejected/<run_id>/*.csv                        -> registros rejeitados + motivo
"""
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import config
from .schema import RAW_SCHEMA, SOURCE_COLUMNS

ACCENTS_FROM = "áàâãäéèêëíìîïóòôõöúùûüçñÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ"
ACCENTS_TO = "aaaaaeeeeiiiiooooouuuucnAAAAAEEEEIIIIOOOOOUUUUCN"

TRUE_VALUES = ["true", "1", "yes", "sim"]


def build_spark(app_name: str = "kindle-ingestion") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(config.SPARK_MASTER)
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
        .config("spark.sql.ansi.enabled", "false")              # cast inválido -> null (tratado como rejeição)
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# --------------------------------------------------------------------------- leitura
def read_source(spark: SparkSession, path: Path) -> DataFrame:
    df = (
        spark.read.option("header", True)
        .option("multiLine", True)        # títulos podem conter quebras de linha
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .csv(str(path), schema=None, inferSchema=False)
    )
    missing = [c for c in SOURCE_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Colunas ausentes no CSV: {missing}")
    # Aplica o schema declarado (todas string nesta fase) na ordem esperada
    return df.select([F.col(f.name).cast(f.dataType) for f in RAW_SCHEMA.fields])


# --------------------------------------------------------------------------- helpers
def _blank(col):
    return F.col(col).isNull() | (F.trim(F.col(col)) == "")


def _bool(col):
    # valores ausentes ou inesperados viram False
    return F.coalesce(F.lower(F.trim(F.col(col))).isin(TRUE_VALUES), F.lit(False))


def _clean_text(col):
    # trim + colapsa espaços múltiplos/quebras de linha
    return F.trim(F.regexp_replace(F.col(col), r"\s+", " "))


def _or_default(expr, default):
    """Retorna `default` quando a expressão é nula ou string vazia."""
    return F.when(expr.isNull() | (expr == ""), F.lit(default)).otherwise(expr)


def _author_key(col):
    norm = F.lower(F.translate(F.col(col), ACCENTS_FROM, ACCENTS_TO))
    norm = F.regexp_replace(norm, r"[^a-z0-9]+", "-")
    norm = F.regexp_replace(norm, r"(^-+|-+$)", "")
    return F.when(norm == "", F.lit("desconhecido")).otherwise(norm)


def _price_range(col):
    expr = F.when(F.col(col).isNull(), F.lit(config.UNKNOWN))
    lower_label_pairs = config.PRICE_RANGES
    expr = expr.when(F.col(col) == 0, F.lit(lower_label_pairs[0][1]))
    for upper, label in lower_label_pairs[1:]:
        if upper is None:
            expr = expr.otherwise(F.lit(label))
        else:
            expr = expr.when(F.col(col) < upper, F.lit(label))
    return expr


# --------------------------------------------------------------------------- validação
def validate(df: DataFrame) -> DataFrame:
    """Converte tipos e adiciona a coluna `reject_reason` (vazia = registro válido)."""
    df = (
        df.withColumn("stars_num", F.col("stars").cast("double"))
        .withColumn("reviews_num", F.col("reviews").cast("double").cast("long"))
        .withColumn("price_num", F.col("price").cast("double"))
        .withColumn("category_id_num", F.col("category_id").cast("double").cast("int"))
    )

    reasons = F.concat_ws(
        ";",
        F.when(_blank("asin"), F.lit("asin_vazio")),
        F.when(~_blank("stars") & F.col("stars_num").isNull(), F.lit("stars_nao_numerico")),
        F.when((F.col("stars_num") < 0) | (F.col("stars_num") > 5), F.lit("stars_fora_0_5")),
        F.when(~_blank("reviews") & F.col("reviews_num").isNull(), F.lit("reviews_nao_numerico")),
        F.when(F.col("reviews_num") < 0, F.lit("reviews_negativo")),
        F.when(~_blank("price") & F.col("price_num").isNull(), F.lit("price_nao_numerico")),
        F.when(F.col("price_num") < 0, F.lit("price_negativo")),
    )
    return df.withColumn("reject_reason", reasons)


# --------------------------------------------------------------------------- transformação
def transform(valid: DataFrame) -> DataFrame:
    max_year = date.today().year + 1

    parsed_date = F.coalesce(
        F.to_date(F.trim("publishedDate"), "yyyy-MM-dd"),
        F.to_date(F.trim("publishedDate"), "MM/dd/yyyy"),
        F.to_date(F.trim("publishedDate"), "dd/MM/yyyy"),
    )

    out = (
        valid
        .withColumn("asin", F.upper(F.trim("asin")))
        .withColumn("title", _or_default(_clean_text("title"), "Sem título"))
        .withColumn("author_name", _or_default(_clean_text("author"), config.UNKNOWN))
        .withColumn("sold_by", _or_default(_clean_text("soldBy"), config.UNKNOWN))
        .withColumn("category_name", _or_default(_clean_text("category_name"), config.UNKNOWN))
        .withColumn("img_url", _or_default(F.trim("imgUrl"), None))
        .withColumn("product_url", _or_default(F.trim("productURL"), None))
        .withColumn("stars", F.coalesce(F.col("stars_num"), F.lit(0.0)))
        .withColumn("reviews", F.coalesce(F.col("reviews_num"), F.lit(0).cast("long")))
        .withColumn("has_reviews", F.col("reviews") > 0)
        .withColumn("price", F.round(F.col("price_num"), 2))
        .withColumn("is_free", F.coalesce(F.col("price") == 0, F.lit(False)))
        .withColumn("price_range", _price_range("price"))
        .withColumn("is_kindle_unlimited", _bool("isKindleUnlimited"))
        .withColumn("is_best_seller", _bool("isBestSeller"))
        .withColumn("is_editors_pick", _bool("isEditorsPick"))
        .withColumn("is_goodreads_choice", _bool("isGoodReadsChoice"))
        .withColumn("category_id", F.col("category_id_num"))
        .withColumn("published_date", parsed_date)
        # datas absurdas (ex.: 1900, 2099) são tratadas como data inválida -> sem data
        .withColumn(
            "published_date",
            F.when(F.year("published_date").between(1950, max_year), F.col("published_date")),
        )
        .withColumn("publication_year", F.year("published_date"))
    )
    out = out.withColumn("author_key", _author_key("author_name"))

    return out.select(
        "asin", "title", "author_name", "author_key", "sold_by", "img_url", "product_url",
        "stars", "reviews", "has_reviews", "price", "price_range", "is_free",
        "is_kindle_unlimited", "is_best_seller", "is_editors_pick", "is_goodreads_choice",
        "category_id", "category_name", "published_date", "publication_year",
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """Mantém um registro por asin (o com mais avaliações; desempate pela data mais recente)."""
    w = Window.partitionBy("asin").orderBy(
        F.col("reviews").desc(), F.col("published_date").desc_nulls_last(), F.col("title")
    )
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


# --------------------------------------------------------------------------- orquestração
def run(spark: SparkSession, source: Path, run_id: str) -> dict:
    raw = read_source(spark, source).cache()
    rows_read = raw.count()
    print(f"[spark] Linhas lidas: {rows_read:,}")

    checked = validate(raw).cache()
    rejected = checked.filter(F.col("reject_reason") != "")
    valid = checked.filter(F.col("reject_reason") == "")

    rejected_dir = config.REJECTED_DIR / run_id
    rejected_count = rejected.count()
    reasons = {}
    if rejected_count:
        (
            rejected.select(*SOURCE_COLUMNS, "reject_reason")
            .coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(str(rejected_dir))
        )
        for row in (
            rejected.select(F.explode(F.split("reject_reason", ";")).alias("r"))
            .groupBy("r").count().collect()
        ):
            reasons[row["r"]] = row["count"]
    print(f"[spark] Rejeitados: {rejected_count:,} {reasons}")

    curated = transform(valid)
    before_dedup = curated.count()
    curated = deduplicate(curated).cache()
    accepted = curated.count()
    duplicates = before_dedup - accepted
    print(f"[spark] Duplicados removidos (asin): {duplicates:,}")

    (
        curated.repartition("publication_year")
        .write.mode("overwrite")
        .partitionBy("publication_year")
        .parquet(str(config.CURATED_DIR))
    )
    print(f"[spark] Parquet escrito em {config.CURATED_DIR} ({accepted:,} livros)")

    raw.unpersist(); checked.unpersist(); curated.unpersist()
    return {
        "rowsRead": rows_read,
        "rowsRejected": rejected_count,
        "rejectedByReason": reasons,
        "duplicatesRemoved": duplicates,
        "rowsAccepted": accepted,
        "rejectedPath": str(rejected_dir) if rejected_count else None,
        "curatedPath": str(config.CURATED_DIR),
    }
