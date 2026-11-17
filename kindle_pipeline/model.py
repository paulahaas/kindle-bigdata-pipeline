"""(Opcional) Modelo scikit-learn para prever o selo Best Seller a partir dos dados no MongoDB.

Uso: python -m kindle_pipeline.model
Saída: outputs/modelo_bestseller.txt
"""
import numpy as np
import pandas as pd
from pymongo.database import Database
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from . import config

NUMERIC = ["price", "stars", "log_reviews", "year", "is_ku", "is_editors_pick", "is_goodreads", "has_reviews"]
CATEGORICAL = ["category"]


def load_frame(db: Database) -> pd.DataFrame:
    cursor = db.books.find({}, {
        "_id": 0, "pricing.price": 1, "rating": 1, "badges": 1,
        "publication.year": 1, "category.name": 1,
    })
    rows = []
    for d in cursor:
        rows.append({
            "price": d["pricing"].get("price"),
            "stars": d["rating"]["stars"],
            "log_reviews": np.log1p(d["rating"]["reviews"]),
            "has_reviews": int(d["rating"]["hasReviews"]),
            "year": d.get("publication", {}).get("year"),
            "is_ku": int(d["badges"]["isKindleUnlimited"]),
            "is_editors_pick": int(d["badges"]["isEditorsPick"]),
            "is_goodreads": int(d["badges"]["isGoodReadsChoice"]),
            "category": d["category"]["name"],
            "target": int(d["badges"]["isBestSeller"]),
        })
    df = pd.DataFrame(rows)
    df["price"] = df["price"].fillna(df["price"].median())
    df["year"] = df["year"].fillna(df["year"].median())
    return df


def train(db: Database) -> None:
    df = load_frame(db)
    positives = int(df["target"].sum())
    if positives < 10 or positives == len(df):
        print(f"[model] Poucos exemplos positivos ({positives}); modelo não treinado.")
        return

    X, y = df[NUMERIC + CATEGORICAL], df["target"]
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.25, stratify=y, random_state=42)

    pipe = Pipeline([
        ("prep", ColumnTransformer([
            ("num", "passthrough", NUMERIC),
            ("cat", OneHotEncoder(handle_unknown="ignore", min_frequency=20), CATEGORICAL),
        ])),
        ("rf", RandomForestClassifier(
            n_estimators=200, min_samples_leaf=5, class_weight="balanced_subsample",
            n_jobs=-1, random_state=42,
        )),
    ])
    pipe.fit(X_tr, y_tr)
    proba = pipe.predict_proba(X_te)[:, 1]
    pred = (proba >= 0.5).astype(int)

    names = pipe.named_steps["prep"].get_feature_names_out()
    imp = pd.Series(pipe.named_steps["rf"].feature_importances_, index=names).sort_values(ascending=False)

    report = [
        "Modelo: RandomForestClassifier (class_weight=balanced_subsample)",
        f"Alvo: badges.isBestSeller | positivos: {positives:,} de {len(df):,} ({positives / len(df):.2%})",
        f"Treino: {len(X_tr):,} | Teste: {len(X_te):,}",
        f"ROC AUC (teste): {roc_auc_score(y_te, proba):.4f}",
        "",
        classification_report(y_te, pred, target_names=["não best seller", "best seller"], digits=3),
        "Top 15 variáveis por importância:",
        imp.head(15).round(4).to_string(),
    ]
    text = "\n".join(report)
    print("\n" + text)
    config.OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    (config.OUTPUTS_DIR / "modelo_bestseller.txt").write_text(text, encoding="utf-8")
    print(f"[model] Relatório salvo em {config.OUTPUTS_DIR / 'modelo_bestseller.txt'}")


if __name__ == "__main__":
    from .mongo_load import get_db
    train(get_db())
