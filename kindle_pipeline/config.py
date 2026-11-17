"""Configurações centrais do pipeline (lidas de variáveis de ambiente)."""
import os
from pathlib import Path

BASE_DIR = Path(os.getenv("PIPELINE_BASE_DIR", Path(__file__).resolve().parent.parent))

DATA_DIR = BASE_DIR / "data"
LANDING_DIR = DATA_DIR / "landing"
CURATED_DIR = DATA_DIR / "curated" / "books"
REJECTED_DIR = DATA_DIR / "rejected"
SAMPLE_FILE = DATA_DIR / "sample" / "kindle_sample.csv"
OUTPUTS_DIR = BASE_DIR / "outputs"

KAGGLE_DATASET = "asaniczka/amazon-kindle-books-dataset-2023-130k-books"
SOURCE_FILENAME = "kindle_data-v2.csv"

# Colunas esperadas no CSV de origem (16 colunas)
SOURCE_COLUMNS = [
    "asin", "title", "author", "soldBy", "imgUrl", "productURL", "stars", "reviews",
    "price", "isKindleUnlimited", "category_id", "isBestSeller", "isEditorsPick",
    "isGoodReadsChoice", "publishedDate", "category_name",
]

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@localhost:27017/?authSource=admin")
MONGO_DB = os.getenv("MONGO_DB", "kindle")

SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "2g")

LOAD_BATCH_SIZE = int(os.getenv("LOAD_BATCH_SIZE", "5000"))

# Número mínimo de avaliações para um autor entrar no ranking
AUTHOR_MIN_REVIEWS = int(os.getenv("AUTHOR_MIN_REVIEWS", "1000"))

UNKNOWN = "Desconhecido"

# Faixas de preço (limite superior exclusivo, rótulo)
PRICE_RANGES = [
    (0.0, "Gratuito"),          # preço == 0
    (3.0, "0.01-2.99"),
    (5.0, "3.00-4.99"),
    (10.0, "5.00-9.99"),
    (20.0, "10.00-19.99"),
    (None, "20.00+"),
]
