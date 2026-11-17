// Consultas para a demonstração ao vivo.
// Execução:
//   docker cp scripts/consultas_demo.js kindle-mongo:/tmp/consultas_demo.js
//   docker exec kindle-mongo mongosh -u admin -p admin123 --authenticationDatabase admin kindle --quiet --file /tmp/consultas_demo.js
// ou cole os blocos no mongosh / MongoDB Compass.

print("\n== Contagem por coleção ==");
["books", "categories", "authors", "ingestion_runs"].forEach(c =>
  print(c.padEnd(16), db.getCollection(c).countDocuments()));

print("\n== Última execução do pipeline (auditoria) ==");
printjson(db.ingestion_runs.find().sort({ startedAt: -1 }).limit(1).toArray()[0]);

print("\n== Exemplo de documento do agregado books ==");
printjson(db.books.findOne({ "rating.hasReviews": true, "badges.isBestSeller": true }) || db.books.findOne());

print("\n== Top 5 categorias por nota média (mín. 100 livros) ==");
db.categories.find({ totalBooks: { $gte: 100 } },
  { totalBooks: 1, avgStars: 1, totalReviews: 1, pctKindleUnlimited: 1 })
  .sort({ avgStars: -1 }).limit(5).forEach(printjson);

print("\n== Efeito do Kindle Unlimited ==");
db.books.aggregate([
  { $group: {
      _id: "$badges.isKindleUnlimited",
      totalLivros: { $sum: 1 },
      notaMedia: { $avg: { $cond: ["$rating.hasReviews", "$rating.stars", null] } },
      avaliacoesMedias: { $avg: "$rating.reviews" } } },
  { $sort: { _id: 1 } }
]).forEach(printjson);

print("\n== Best sellers por faixa de preço ==");
db.books.aggregate([
  { $group: { _id: "$pricing.priceRange", livros: { $sum: 1 },
      bestSellers: { $sum: { $cond: ["$badges.isBestSeller", 1, 0] } } } },
  { $sort: { _id: 1 } }
]).forEach(printjson);

print("\n== Top 10 autores (mín. 1000 avaliações) ==");
db.authors.find({ totalReviews: { $gte: 1000 }, _id: { $ne: "desconhecido" } },
  { name: 1, totalBooks: 1, totalReviews: 1, avgStars: 1 })
  .sort({ avgStars: -1, totalReviews: -1 }).limit(10).forEach(printjson);

print("\n== Busca textual no título usando o índice de texto ==");
db.books.find({ $text: { $search: "dragon" } }, { title: 1, "author.name": 1, "rating.stars": 1 })
  .limit(5).forEach(printjson);

print("\n== O plano de execução confirma que o índice foi usado ==");
printjson(db.books.find({ "category.name": "Romance" }).sort({ "rating.stars": -1 })
  .explain("queryPlanner").queryPlanner.winningPlan);

print("\n== O validador $jsonSchema rejeita um documento inválido (stars = 9) ==");
try {
  db.books.insertOne({ _id: "INVALIDO", asin: "INVALIDO", title: "x", soldBy: "x",
    author: { name: "x", authorKey: "x" }, category: { name: "x" },
    pricing: { price: 1.0, priceRange: "0.01-2.99", isFree: false },
    rating: { stars: 9.0, reviews: NumberInt(1), hasReviews: true },
    badges: { isKindleUnlimited: false, isBestSeller: false, isEditorsPick: false, isGoodReadsChoice: false },
    publication: { date: null, year: null } });
  print("ERRO: documento inválido foi aceito");
} catch (e) { print("OK, documento rejeitado pelo validador: " + (e.errmsg || e.message)); }
