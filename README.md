teste
    indent
fim# Pipeline Big Data: Amazon Kindle Books 2023 → MongoDB

**Avaliação N3, Desenvolvimento com Linguagens Disruptivas I**
Tema: Fluxo de Big Data para carga de dados em um BD NoSQL.

**Equipe 03:** Yasmin Ferreira, Paula Haas, Marcella Cagliari e Gustavo Correia da Silveira.

**Dataset:** [Amazon Kindle Books Dataset 2023 (130k books)](https://www.kaggle.com/datasets/asaniczka/amazon-kindle-books-dataset-2023-130k-books), Kaggle, cerca de 130 mil livros e 16 colunas.

Este repositório implementa o pipeline de ingestão especificado na N2. O objetivo é dar a autores independentes orientações baseadas em dados sobre preço, categoria, participação no Kindle Unlimited (KU) e selos.

---

## Arquitetura

```mermaid
flowchart LR
    K[Kaggle<br/>kagglehub] -->|1. download| L[(data/landing<br/>CSV original, imutável)]
    L -->|2. leitura com schema declarado| S[PySpark]
    S -->|3. validação| R[(data/rejected<br/>CSV + motivo)]
    S -->|4. limpeza e transformação<br/>5. Parquet particionado por ano| C[(data/curated/books<br/>publication_year=AAAA)]
    C -->|6. upsert por asin| B[(MongoDB<br/>books)]
    B -->|aggregation $merge| CA[(categories)]
    B -->|aggregation $merge| AU[(authors)]
    S -.->|7. métricas da execução| IR[(ingestion_runs)]
    B --> AN[Análises<br/>pandas + matplotlib]
    B --> ML[Modelo Best Seller<br/>scikit-learn]
```

| Etapa | O que acontece | Onde está no código |
|---|---|---|
| 1. Download | `kagglehub` baixa o CSV para `data/landing/`, que fica somente leitura e nunca é alterado | `kindle_pipeline/download.py` |
| 2. Leitura | Spark lê o CSV (multiLine, aspas escapadas) e confere as 16 colunas do schema declarado | `spark_ingest.read_source` |
| 3. Validação | Converte os tipos e marca o motivo da rejeição. Linhas rejeitadas vão para `data/rejected/<run_id>/` | `spark_ingest.validate` |
| 4. Transformação | Trim e normalização de textos, datas, campos derivados e deduplicação por `asin` | `spark_ingest.transform`, `deduplicate` |
| 5. Camada curated | Parquet em `data/curated/books/`, particionado por `publication_year` | `spark_ingest.run` |
| 6. Carga | `ReplaceOne(upsert=True)` por `asin` em lotes de 5.000. As coleções `categories` e `authors` são montadas por aggregation com `$merge` | `mongo_load.py` |
| 7. Índices e auditoria | Índices, validadores `$jsonSchema` e registro de cada execução em `ingestion_runs` | `mongo_schema.py`, `run.py` |

## Regras de validação e limpeza

| Situação | Ação |
|---|---|
| `asin` vazio | **rejeita** (`asin_vazio`) |
| `stars` fora de 0–5 ou não numérico | **rejeita** (`stars_fora_0_5` / `stars_nao_numerico`) |
| `reviews` negativo ou não numérico | **rejeita** (`reviews_negativo` / `reviews_nao_numerico`) |
| `price` negativo ou não numérico | **rejeita** (`price_negativo` / `price_nao_numerico`) |
| `asin` duplicado | mantém **um** registro (o com mais avaliações e, no empate, a data mais recente) |
| Livro gratuito (`price = 0`) | mantém, com `pricing.isFree = true` e faixa `Gratuito` |
| Sem avaliações (`reviews = 0`) | mantém, com `rating.hasReviews = false`. Fica fora da média de estrelas |
| Data inválida ou fora de 1950–ano atual+1 | mantém, com `publication.date = null` |
| `author`, `soldBy` ou `category_name` vazios | preenche com `"Desconhecido"` |
| Textos | trim e espaços/quebras de linha colapsados |

**Campos derivados:** `publication.year`, `pricing.priceRange`, `pricing.isFree`, `rating.hasReviews` e `author.authorKey` (nome normalizado, sem acentos, em minúsculas e com hífens, por exemplo `jose-da-silva`).

**Faixas de preço (US$):** `Gratuito`, `0.01-2.99`, `3.00-4.99`, `5.00-9.99`, `10.00-19.99`, `20.00+`.

## Modelo de dados (MongoDB, banco `kindle`)

`books` é o agregado principal. Preço, avaliação, selos, publicação e categoria ficam embutidos como subdocumentos:

```json
{
  "_id": "B0BXXXXXXX",
  "asin": "B0BXXXXXXX",
  "title": "...",
  "author":      { "name": "Jane Doe", "authorKey": "jane-doe" },
  "soldBy": "Amazon.com Services LLC",
  "imgUrl": "https://...", "productUrl": "https://...",
  "category":    { "id": 6, "name": "Romance" },
  "pricing":     { "price": 4.99, "priceRange": "3.00-4.99", "isFree": false },
  "rating":      { "stars": 4.5, "reviews": 1523, "hasReviews": true },
  "badges":      { "isKindleUnlimited": true, "isBestSeller": false,
                   "isEditorsPick": false, "isGoodReadsChoice": false },
  "publication": { "date": ISODate("2021-05-04"), "year": 2021 },
  "ingestion":   { "runId": "20261117T130000Z-ab12cd", "loadedAt": ISODate("...") }
}
```

| Coleção | `_id` | Conteúdo |
|---|---|---|
| `books` | `asin` | um documento por livro (acima) |
| `categories` | nome da categoria | `totalBooks`, `totalReviews`, `avgStars`, `avgReviews`, `avgPrice` e `pctKindleUnlimited`/`pctBestSeller`/`pctEditorsPick`/`pctGoodReadsChoice` (percentuais de 0 a 100) |
| `authors` | `authorKey` | `name`, `totalBooks`, `totalReviews`, `avgStars`, `avgPrice`, `bestSellerCount`, `kindleUnlimitedCount`, `categories[]`, `firstYear`, `lastYear` |
| `ingestion_runs` | id da execução | `status`, `startedAt`, `finishedAt`, `durationSeconds`, `rowsRead`, `rowsAccepted`, `rowsRejected`, `duplicatesRemoved`, `rejectedByReason`, `booksUpserted` |

Todas as coleções têm validador `$jsonSchema` (`validationLevel: strict`). Os índices de `books` cobrem `asin` (único), `category.name + rating.stars`, `author.authorKey`, `publication.year + category.name`, `pricing.priceRange`, `badges.isKindleUnlimited`, `badges.isBestSeller` e `rating.reviews`, além de um índice de texto em `title`.

## Estrutura do repositório

```
.
├── docker-compose.yml        # MongoDB 7 + mongo-express + container do pipeline
├── Dockerfile                # Python 3.11 + Java 17 + PySpark
├── requirements.txt
├── .env.example
├── kindle_pipeline/
│   ├── run.py                # orquestrador (ponto de entrada)
│   ├── config.py             # caminhos, conexão, faixas de preço
│   ├── download.py           # etapa 1: Kaggle -> landing
│   ├── schema.py             # schema declarado (origem e curated)
│   ├── spark_ingest.py       # etapas 2-5: Spark (validação, transformação, Parquet)
│   ├── mongo_schema.py       # validadores $jsonSchema e índices
│   ├── mongo_load.py         # etapa 6: upsert e agregações categories/authors
│   ├── analyses.py           # análises e gráficos
│   ├── model.py              # (opcional) modelo para prever Best Seller
│   └── make_sample.py        # gera amostra sintética para teste rápido
├── scripts/consultas_demo.js # consultas mongosh para a apresentação
├── data/
│   ├── landing/              # CSV original (não versionado)
│   ├── curated/books/        # Parquet (gerado)
│   ├── rejected/             # rejeitados por execução (gerado)
│   └── sample/kindle_sample.csv
└── outputs/                  # CSVs, gráficos e relatórios (gerados)
```

---

## Como executar

### Pré-requisitos

- **Docker** e **Docker Compose** (opção A, recomendada), **ou**
- **Python 3.10–3.12** e **Java 17** com Docker apenas para o MongoDB (opção B). O PySpark 3.5 não suporta Python 3.13.
- Para baixar o dataset completo, uma conta no Kaggle: em *Settings → API → Create New Token*, coloque o token em `~/.kaggle/kaggle.json` ou nas variáveis `KAGGLE_USERNAME`/`KAGGLE_KEY`. Também dá para baixar o CSV manualmente pelo site (ver abaixo).

### Opção A: tudo com Docker

```bash
git clone https://github.com/paulahaas/kindle-bigdata-pipeline.git
cd kindle-bigdata-pipeline
cp .env.example .env              # preencha KAGGLE_USERNAME e KAGGLE_KEY

# 1) sobe o MongoDB e o mongo-express (http://localhost:8081)
docker compose up -d mongo mongo-express

# 2) constrói a imagem e roda o pipeline completo + análises + modelo
docker compose --profile pipeline build
docker compose run --rm pipeline --analyses

# teste rápido com a amostra sintética (não precisa de Kaggle)
docker compose run --rm pipeline --sample --analyses
```

### Opção B: Python local com MongoDB no Docker

```bash
docker compose up -d mongo mongo-express

python3.11 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python -m kindle_pipeline.run --analyses              # dataset completo (baixa via Kaggle)
python -m kindle_pipeline.run --sample --analyses     # amostra sintética
```

Se preferir baixar manualmente, salve `kindle_data-v2.csv` do Kaggle em `data/landing/` e rode `python -m kindle_pipeline.run --analyses`. O arquivo existente é reaproveitado. Para usar outro caminho: `--input caminho/arquivo.csv`.

Para usar o **MongoDB Atlas** em vez do Docker, defina `MONGO_URI` (por exemplo `export MONGO_URI="mongodb+srv://usuario:senha@cluster.xxxx.mongodb.net/"`).

### Comandos individuais

```bash
python -m kindle_pipeline.download     # só o download
python -m kindle_pipeline.make_sample  # (re)gera a amostra sintética
python -m kindle_pipeline.analyses     # só as análises (lê do MongoDB)
python -m kindle_pipeline.model        # só o modelo
```

### Consultas para a demonstração

```bash
docker cp scripts/consultas_demo.js kindle-mongo:/tmp/consultas_demo.js
docker exec kindle-mongo mongosh -u admin -p admin123 --authenticationDatabase admin kindle --quiet --file /tmp/consultas_demo.js
```

O script mostra as contagens, a última execução, um documento de exemplo, as análises principais, o uso de índice (`explain`) e o validador rejeitando um documento inválido.

---

## Saídas esperadas

### 1. Console

Saída do pipeline com a **amostra sintética** (valores exatos; os tempos variam):

```
[spark] Linhas lidas: 1,001
[spark] Rejeitados: 6 {'asin_vazio': 1, 'stars_fora_0_5': 1, 'reviews_negativo': 1, 'price_negativo': 1, 'stars_nao_numerico': 1, 'price_nao_numerico': 1}
[spark] Duplicados removidos (asin): 1
[spark] Parquet escrito em .../data/curated/books (994 livros)
[mongo] books: 994 upserts, 0 removidos por não estarem mais na fonte
[mongo] Agregados: 10 categorias, 43 autores
[mongo] Índices criados

=== Resumo da execução ===
  linhas lidas        : 1,001
  rejeitadas          : 6
  duplicadas removidas: 1
  aceitas (books)     : 994
```

Com o **dataset completo** do Kaggle, espere em torno de 130 mil linhas lidas, quase todas aceitas, distribuídas em algumas dezenas de categorias. As contagens exatas ficam registradas em `ingestion_runs`.

### 2. Arquivos

| Caminho | Conteúdo |
|---|---|
| `data/landing/kindle_data-v2.csv` | CSV original (somente leitura) |
| `data/curated/books/publication_year=AAAA/part-*.parquet` | livros válidos, particionados por ano (`publication_year=__HIVE_DEFAULT_PARTITION__` = sem data) |
| `data/rejected/<run_id>/part-*.csv` | linhas rejeitadas com a coluna `reject_reason` |
| `outputs/01_desempenho_categorias.csv` e `01_categorias_nota_media.png` | nota média, avaliações e %KU/%Best Seller por categoria |
| `outputs/02_efeito_kindle_unlimited.csv` e `02_kindle_unlimited.png` | KU × nota média e avaliações médias |
| `outputs/03_preco_vs_selos.csv` e `03_preco_vs_selos.png` | % de Best Seller, Editors' Pick e GoodReads Choice por faixa de preço |
| `outputs/04_publicacoes_ano_categoria.csv` e `04_publicacoes_por_ano.png` | publicações por ano e categoria |
| `outputs/05_ranking_autores.csv` e `05_ranking_autores.png` | top autores por nota, com mínimo de 1.000 avaliações (`AUTHOR_MIN_REVIEWS`) |
| `outputs/relatorio.md` | resumo de todas as análises com tabelas e gráficos |
| `outputs/modelo_bestseller.txt` | ROC AUC, classification report e importância das variáveis |

### 3. MongoDB

Banco `kindle` com as coleções `books`, `categories`, `authors` e `ingestion_runs`. Elas podem ser vistas no mongo-express (http://localhost:8081) ou no Compass (`mongodb://admin:admin123@localhost:27017/?authSource=admin`). O pipeline é **idempotente**: rodar de novo atualiza os documentos por `asin` sem duplicar nada e acrescenta mais um registro em `ingestion_runs`.

---

## Roteiro da apresentação (17/11)

1. Mostrar a arquitetura (diagrama acima) e o CSV em `data/landing/`.
2. Rodar `docker compose run --rm pipeline --sample --analyses` e comentar o resumo (lidas, rejeitadas, duplicadas, aceitas).
3. Abrir `data/rejected/<run_id>/` e mostrar os motivos de rejeição.
4. Mostrar as partições Parquet em `data/curated/books/`.
5. No mongo-express ou Compass: um documento de `books`, depois `categories`, `authors` e `ingestion_runs`.
6. Rodar `scripts/consultas_demo.js`: análises, `explain` com índice e validador rejeitando documento.
7. Abrir `outputs/relatorio.md` com os gráficos e `outputs/modelo_bestseller.txt`.
8. Opcional: mostrar a execução com o dataset completo, já feita antes da aula porque leva alguns minutos.

## Ajustes em relação ao documento da N2

Na implementação, corrigimos algumas inconsistências da especificação:

- A agregação de exemplo do efeito do KU não tinha `$avg` nem aspas nos caminhos. A versão corrigida está em `analyses.KU_PIPELINE`.
- Os tipos foram padronizados: `reviews` é inteiro em todas as coleções e o campo é `rating.stars` (não `rating.star`).
- Os nomes foram unificados: `priceRange` (não `priceRanger`), `productUrl` (não `produtoUrl`), `authorKey` (não `autor_id`).
- `pctKindleUnlimited` e `pctBestSeller` são numéricos (percentuais de 0 a 100), não booleanos.
- As coleções seguem o texto da N2: `categories` e `authors`.

## Tecnologias

PySpark 3.5 · Parquet · MongoDB 7 (Docker ou Atlas) · PyMongo · kagglehub · pandas · NumPy · matplotlib · scikit-learn · Docker Compose · mongo-express / MongoDB Compass
