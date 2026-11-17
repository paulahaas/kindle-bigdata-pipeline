"""Gera uma amostra SINTÉTICA no mesmo formato do CSV do Kaggle.

Serve para testar/demonstrar o pipeline sem baixar o dataset completo. Inclui de
propósito registros com problemas para exercitar as regras de validação:
asin vazio, stars fora de 0-5, reviews/price negativos, valores não numéricos,
asin duplicado, autor vazio, data inválida e livros gratuitos/sem avaliações.
"""
import csv
import random
import sys
from datetime import date, timedelta
from pathlib import Path

from .config import SOURCE_COLUMNS

CATEGORIES = [
    (1, "Literature & Fiction"), (2, "Mystery, Thriller & Suspense"), (3, "Romance"),
    (4, "Science Fiction & Fantasy"), (5, "Teen & Young Adult"), (6, "Biographies & Memoirs"),
    (7, "Business & Money"), (8, "Self-Help"), (9, "Children's eBooks"), (10, "History"),
]
AUTHORS = [f"Autor Exemplo {i}" for i in range(1, 41)] + ["José da Silva", "Ana Conceição"]
PUBLISHERS = ["Amazon.com Services LLC", "Penguin Group (USA) LLC", "HarperCollins Publishers",
              "Random House LLC", "Simon and Schuster Digital Sales LLC"]
WORDS = ["Shadow", "River", "Secret", "Last", "Garden", "Night", "Empire", "Letters", "Silent",
         "Promise", "Winter", "House", "Code", "Journey", "Storm", "Kingdom", "Memory", "Fire"]


def _row(i: int, rnd: random.Random) -> dict:
    cat_id, cat_name = rnd.choice(CATEGORIES)
    reviews = 0 if rnd.random() < 0.15 else int(rnd.paretovariate(1.2) * 20)
    stars = 0.0 if reviews == 0 else round(rnd.uniform(3.0, 5.0), 1)
    price = 0.0 if rnd.random() < 0.05 else round(rnd.choice([0.99, 2.99, 3.99, 4.99, 7.99, 9.99, 12.99, 14.99, 24.99]), 2)
    pub = date(2000, 1, 1) + timedelta(days=rnd.randint(0, 8700))
    return {
        "asin": f"TEST{i:06d}",
        "title": " ".join(rnd.sample(WORDS, rnd.randint(2, 4))),
        "author": rnd.choice(AUTHORS),
        "soldBy": rnd.choice(PUBLISHERS),
        "imgUrl": f"https://example.com/img/TEST{i:06d}.jpg",
        "productURL": f"https://example.com/dp/TEST{i:06d}",
        "stars": stars,
        "reviews": reviews,
        "price": price,
        "isKindleUnlimited": rnd.random() < 0.3,
        "category_id": cat_id,
        "isBestSeller": reviews > 500 and rnd.random() < 0.4,
        "isEditorsPick": rnd.random() < 0.03,
        "isGoodReadsChoice": rnd.random() < 0.02,
        "publishedDate": pub.isoformat(),
        "category_name": cat_name,
    }


def generate(path: Path, n: int = 1000, seed: int = 42) -> Path:
    rnd = random.Random(seed)
    rows = [_row(i, rnd) for i in range(1, n + 1)]

    # --- registros problemáticos (cada um exercita uma regra) ---
    rows[10]["asin"] = ""                      # rejeitar: asin vazio
    rows[20]["stars"] = 7.5                    # rejeitar: stars fora de 0-5
    rows[30]["reviews"] = -3                   # rejeitar: reviews negativo
    rows[40]["price"] = -1.99                  # rejeitar: price negativo
    rows[50]["stars"] = "cinco"                # rejeitar: stars não numérico
    rows[60]["price"] = "N/A"                  # rejeitar: price não numérico
    rows[70]["author"] = ""                    # manter: autor -> "Desconhecido"
    rows[80]["soldBy"] = "   "                 # manter: soldBy -> "Desconhecido"
    rows[90]["publishedDate"] = "2023-13-45"   # manter: data inválida -> sem data
    rows[95]["publishedDate"] = ""             # manter: sem data
    rows[100]["title"] = "  Título   com\n quebra de linha  "  # manter: texto normalizado
    dup = dict(rows[5]); dup["reviews"] = 0
    rows.append(dup)                           # manter um: asin duplicado

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SOURCE_COLUMNS, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        w.writerows(rows)
    print(f"[sample] {len(rows)} linhas sintéticas escritas em {path}")
    return path


if __name__ == "__main__":
    from . import config
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else config.SAMPLE_FILE
    generate(out)
