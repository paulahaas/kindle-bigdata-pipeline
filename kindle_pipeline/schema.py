"""Schema declarado do CSV de origem e schema tipado da camada curated."""
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType,
)

from .config import SOURCE_COLUMNS  # noqa: E402  (16 colunas, lidas como string)

RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in SOURCE_COLUMNS])

# Schema final da camada curated (Parquet)
CURATED_SCHEMA = StructType([
    StructField("asin", StringType(), False),
    StructField("title", StringType(), False),
    StructField("author_name", StringType(), False),
    StructField("author_key", StringType(), False),
    StructField("sold_by", StringType(), False),
    StructField("img_url", StringType(), True),
    StructField("product_url", StringType(), True),
    StructField("stars", DoubleType(), False),
    StructField("reviews", LongType(), False),
    StructField("has_reviews", BooleanType(), False),
    StructField("price", DoubleType(), True),
    StructField("price_range", StringType(), False),
    StructField("is_free", BooleanType(), False),
    StructField("is_kindle_unlimited", BooleanType(), False),
    StructField("is_best_seller", BooleanType(), False),
    StructField("is_editors_pick", BooleanType(), False),
    StructField("is_goodreads_choice", BooleanType(), False),
    StructField("category_id", IntegerType(), True),
    StructField("category_name", StringType(), False),
    StructField("published_date", DateType(), True),
    StructField("publication_year", IntegerType(), True),  # coluna de partição
])
