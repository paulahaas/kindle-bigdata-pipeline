"""Etapa 1: download do dataset do Kaggle para a camada landing/.

O arquivo original nunca é alterado: se já existir em landing/, ele é reaproveitado.
"""
import hashlib
import shutil
from pathlib import Path

from . import config


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def download_dataset(force: bool = False) -> Path:
    target = config.LANDING_DIR / config.SOURCE_FILENAME
    config.LANDING_DIR.mkdir(parents=True, exist_ok=True)

    if target.exists() and not force:
        print(f"[download] Arquivo já existe em landing/, reaproveitando: {target}")
        return target

    try:
        import kagglehub
    except ImportError as exc:
        raise SystemExit(
            "kagglehub não instalado. Rode `pip install -r requirements.txt` "
            f"ou baixe manualmente {config.SOURCE_FILENAME} para {config.LANDING_DIR}"
        ) from exc

    print(f"[download] Baixando {config.KAGGLE_DATASET} via kagglehub...")
    folder = Path(kagglehub.dataset_download(config.KAGGLE_DATASET))
    candidates = list(folder.rglob(config.SOURCE_FILENAME)) or list(folder.rglob("*.csv"))
    if not candidates:
        raise SystemExit(f"Nenhum CSV encontrado em {folder}")

    source = max(candidates, key=lambda p: p.stat().st_size)
    if target.exists():
        target.chmod(0o644)  # libera escrita só para substituir no --force
    shutil.copy2(source, target)
    target.chmod(0o444)  # somente leitura: a landing é imutável
    print(f"[download] Copiado para {target} (sha256={sha256(target)[:12]}...)")
    return target


if __name__ == "__main__":
    download_dataset()
