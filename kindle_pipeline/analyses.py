"""Análises planejadas na N2, executadas como agregações no MongoDB.

Cada análise gera um CSV e um gráfico PNG em outputs/, e um resumo em outputs/relatorio.md.

Uso: python -m kindle_pipeline.analyses
"""
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from pymongo.database import Database  # noqa: E402

from . import config  # noqa: E402

MIN_BOOKS_PER_CATEGORY = 20


def _ratio(field):
    """Acumulador ($group): fração de documentos em que o campo booleano é true."""
    return {"$avg": {"$cond": [f"${field}", 1, 0]}}


def _to_pct(name):
    """Expressão ($project): fração -> percentual 0-100 com 2 casas."""
    return {"$round": [{"$multiply": [f"${name}", 100]}, 2]}


# 1. Desempenho por categoria (lê da coleção pré-calculada `categories`)
def category_performance(db: Database) -> pd.DataFrame:
    rows = db.categories.find(
        {"totalBooks": {"$gte": MIN_BOOKS_PER_CATEGORY}},
        {"_id": 1, "totalBooks": 1, "avgStars": 1, "totalReviews": 1, "avgReviews": 1,
         "avgPrice": 1, "pctKindleUnlimited": 1, "pctBestSeller": 1},
    ).sort([("avgStars", -1), ("totalReviews", -1)])
    return pd.DataFrame(list(rows)).rename(columns={"_id": "categoria"})


# 2. Efeito do Kindle Unlimited (versão corrigida da agregação da N2: $avg + caminhos entre aspas)
KU_PIPELINE = [
    {"$group": {
        "_id": "$badges.isKindleUnlimited",
        "totalLivros": {"$sum": 1},
        "notaMedia": {"$avg": {"$cond": ["$rating.hasReviews", "$rating.stars", None]}},
        "avaliacoesMedias": {"$avg": "$rating.reviews"},
        "precoMedio": {"$avg": "$pricing.price"},
        "pctComAvaliacoes": _ratio("rating.hasReviews"),
        "pctBestSeller": _ratio("badges.isBestSeller"),
    }},
    {"$project": {
        "_id": 0,
        "kindleUnlimited": "$_id",
        "totalLivros": 1,
        "notaMedia": {"$round": ["$notaMedia", 3]},
        "avaliacoesMedias": {"$round": ["$avaliacoesMedias", 1]},
        "precoMedio": {"$round": ["$precoMedio", 2]},
        "pctComAvaliacoes": _to_pct("pctComAvaliacoes"),
        "pctBestSeller": _to_pct("pctBestSeller"),
    }},
    {"$sort": {"kindleUnlimited": 1}},
]


def kindle_unlimited_effect(db: Database) -> pd.DataFrame:
    return pd.DataFrame(list(db.books.aggregate(KU_PIPELINE)))


# 3. Faixas de preço x selos
PRICE_ORDER = ["Gratuito"] + [label for _, label in config.PRICE_RANGES[1:]] + [config.UNKNOWN]

PRICE_PIPELINE = [
    {"$group": {
        "_id": "$pricing.priceRange",
        "totalLivros": {"$sum": 1},
        "notaMedia": {"$avg": {"$cond": ["$rating.hasReviews", "$rating.stars", None]}},
        "pctBestSeller": _ratio("badges.isBestSeller"),
        "pctEditorsPick": _ratio("badges.isEditorsPick"),
        "pctGoodReadsChoice": _ratio("badges.isGoodReadsChoice"),
        "pctKindleUnlimited": _ratio("badges.isKindleUnlimited"),
    }},
    {"$project": {"_id": 0, "faixaPreco": "$_id", "totalLivros": 1,
                  "notaMedia": {"$round": ["$notaMedia", 3]},
                  "pctBestSeller": _to_pct("pctBestSeller"), "pctEditorsPick": _to_pct("pctEditorsPick"),
                  "pctGoodReadsChoice": _to_pct("pctGoodReadsChoice"),
                  "pctKindleUnlimited": _to_pct("pctKindleUnlimited")}},
]


def price_vs_badges(db: Database) -> pd.DataFrame:
    df = pd.DataFrame(list(db.books.aggregate(PRICE_PIPELINE)))
    if df.empty:
        return df
    df["ordem"] = df["faixaPreco"].apply(lambda x: PRICE_ORDER.index(x) if x in PRICE_ORDER else 99)
    return df.sort_values("ordem").drop(columns="ordem").reset_index(drop=True)


# 4. Publicações por ano e por categoria
YEAR_PIPELINE = [
    {"$match": {"publication.year": {"$ne": None}}},
    {"$group": {"_id": {"ano": "$publication.year", "categoria": "$category.name"}, "livros": {"$sum": 1}}},
    {"$project": {"_id": 0, "ano": "$_id.ano", "categoria": "$_id.categoria", "livros": 1}},
    {"$sort": {"ano": 1, "livros": -1}},
]


def publications_per_year(db: Database) -> pd.DataFrame:
    return pd.DataFrame(list(db.books.aggregate(YEAR_PIPELINE, allowDiskUse=True)))


# 5. Ranking de autores (com mínimo de avaliações para evitar notas infladas)
def author_ranking(db: Database, min_reviews: int = config.AUTHOR_MIN_REVIEWS, top: int = 20) -> pd.DataFrame:
    rows = db.authors.find(
        {"totalReviews": {"$gte": min_reviews}, "_id": {"$ne": "desconhecido"}},
        {"_id": 0, "name": 1, "totalBooks": 1, "totalReviews": 1, "avgStars": 1, "bestSellerCount": 1},
    ).sort([("avgStars", -1), ("totalReviews", -1)]).limit(top)
    return pd.DataFrame(list(rows))


# --------------------------------------------------------------------------- gráficos
def _save(fig, name):
    path = config.OUTPUTS_DIR / name
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)
    return path


def _plot_all(cat, ku, price, years, authors):
    if not cat.empty:
        top = cat.nlargest(15, "totalBooks").sort_values("avgStars")
        fig, ax = plt.subplots(figsize=(9, 6))
        ax.barh(top["categoria"], top["avgStars"], color="#3b6ea5")
        ax.set_xlim(max(0, top["avgStars"].min() - 0.3), 5)
        ax.set_xlabel("Nota média (livros com avaliações)")
        ax.set_title("Nota média das 15 maiores categorias")
        _save(fig, "01_categorias_nota_media.png")

    if not ku.empty:
        fig, axes = plt.subplots(1, 2, figsize=(9, 4))
        labels = ["Fora do KU" if not k else "No KU" for k in ku["kindleUnlimited"]]
        axes[0].bar(labels, ku["notaMedia"], color=["#999999", "#e07b39"])
        axes[0].set_title("Nota média")
        axes[0].set_ylim(max(0, ku["notaMedia"].min() - 0.3), 5)
        axes[1].bar(labels, ku["avaliacoesMedias"], color=["#999999", "#e07b39"])
        axes[1].set_title("Avaliações médias por livro")
        fig.suptitle("Efeito do Kindle Unlimited")
        _save(fig, "02_kindle_unlimited.png")

    if not price.empty:
        fig, ax = plt.subplots(figsize=(9, 4.5))
        x = range(len(price))
        w = 0.27
        ax.bar([i - w for i in x], price["pctBestSeller"], w, label="Best Seller")
        ax.bar(list(x), price["pctEditorsPick"], w, label="Editors' Pick")
        ax.bar([i + w for i in x], price["pctGoodReadsChoice"], w, label="GoodReads Choice")
        ax.set_xticks(list(x)); ax.set_xticklabels(price["faixaPreco"], rotation=20)
        ax.set_ylabel("% dos livros da faixa")
        ax.set_title("Selos por faixa de preço (US$)")
        ax.legend()
        _save(fig, "03_preco_vs_selos.png")

    if not years.empty:
        per_year = years.groupby("ano")["livros"].sum()
        per_year = per_year[per_year.index >= 2000]
        fig, ax = plt.subplots(figsize=(9, 4))
        ax.plot(per_year.index, per_year.values, marker="o", color="#3b6ea5")
        ax.set_title("Livros publicados por ano (desde 2000)")
        ax.set_xlabel("Ano"); ax.set_ylabel("Livros")
        _save(fig, "04_publicacoes_por_ano.png")

    if not authors.empty:
        top = authors.head(15).iloc[::-1]
        fig, ax = plt.subplots(figsize=(9, 6))
        ax.barh(top["name"], top["avgStars"], color="#4a9a6b")
        ax.set_xlim(max(0, top["avgStars"].min() - 0.2), 5)
        ax.set_title(f"Top autores por nota (mín. {config.AUTHOR_MIN_REVIEWS:,} avaliações)")
        _save(fig, "05_ranking_autores.png")


def _md_table(df: pd.DataFrame, n: int = 15) -> str:
    if df.empty:
        return "_sem dados_\n"
    d = df.head(n)
    header = "| " + " | ".join(map(str, d.columns)) + " |"
    sep = "| " + " | ".join("---" for _ in d.columns) + " |"
    body = "\n".join("| " + " | ".join("" if pd.isna(v) else str(v) for v in r) + " |" for r in d.itertuples(index=False))
    return f"{header}\n{sep}\n{body}\n"


def run_all(db: Database) -> None:
    config.OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    results = {
        "01_desempenho_categorias": category_performance(db),
        "02_efeito_kindle_unlimited": kindle_unlimited_effect(db),
        "03_preco_vs_selos": price_vs_badges(db),
        "04_publicacoes_ano_categoria": publications_per_year(db),
        "05_ranking_autores": author_ranking(db),
    }
    for name, df in results.items():
        df.to_csv(config.OUTPUTS_DIR / f"{name}.csv", index=False)
        print(f"\n--- {name} ---")
        print(df.head(10).to_string(index=False) if not df.empty else "(sem dados)")

    _plot_all(*results.values())

    last_run = db.ingestion_runs.find_one({"status": "success"}, sort=[("startedAt", -1)]) or {}
    years = results["04_publicacoes_ano_categoria"]
    per_year = (years.groupby("ano", as_index=False)["livros"].sum().sort_values("ano", ascending=False)
                if not years.empty else years)
    md = [
        "# Relatório de análises: Amazon Kindle Books 2023\n",
        f"Execução: `{last_run.get('_id', '-')}` | lidas: {last_run.get('rowsRead', '-')} | "
        f"aceitas: {last_run.get('rowsAccepted', '-')} | rejeitadas: {last_run.get('rowsRejected', '-')} | "
        f"duplicadas: {last_run.get('duplicatesRemoved', '-')}\n",
        "## 1. Desempenho por categoria\n", _md_table(results["01_desempenho_categorias"]),
        "![](01_categorias_nota_media.png)\n",
        "## 2. Efeito do Kindle Unlimited\n", _md_table(results["02_efeito_kindle_unlimited"]),
        "![](02_kindle_unlimited.png)\n",
        "## 3. Faixa de preço x selos\n", _md_table(results["03_preco_vs_selos"]),
        "![](03_preco_vs_selos.png)\n",
        "## 4. Publicações por ano (anos mais recentes)\n", _md_table(per_year),
        "![](04_publicacoes_por_ano.png)\n",
        f"## 5. Ranking de autores (mín. {config.AUTHOR_MIN_REVIEWS:,} avaliações)\n",
        _md_table(results["05_ranking_autores"], 20),
        "![](05_ranking_autores.png)\n",
    ]
    (config.OUTPUTS_DIR / "relatorio.md").write_text("\n".join(md), encoding="utf-8")
    print(f"\n[analyses] CSVs, gráficos e relatorio.md salvos em {config.OUTPUTS_DIR}")


if __name__ == "__main__":
    from .mongo_load import get_db
    run_all(get_db())
