
# NoSQL Analysis for FlexiMart Product Catalog

## Section A: Limitations of RDBMS

Relational databases like MySQL are designed for structured, tabular data with a fixed schema. This poses several challenges for FlexiMart’s expanding product catalog:

1. **Diverse Product Attributes:** RDBMS tables require predefined columns. If products have different attributes (e.g., laptops with RAM/processor, shoes with size/color), the schema must either include many nullable columns or use complex join tables, leading to inefficient storage and complicated queries.
2. **Frequent Schema Changes:** Adding new product types with unique attributes requires altering the table structure, which can be disruptive, time-consuming, and may impact existing applications.
3. **Nested Data (Customer Reviews):** Storing customer reviews as nested data is difficult in RDBMS. Reviews typically require a separate table and foreign keys, making it hard to retrieve a product along with all its reviews in a single query.

These limitations make it challenging to support a highly diverse and rapidly evolving product catalog using a traditional relational database.

---

## Section B: NoSQL Benefits

MongoDB, a NoSQL document database, addresses these challenges effectively:

1. **Flexible Schema:** MongoDB stores data as JSON-like documents, allowing each product to have its own set of attributes. There is no need for a fixed schema, so new product types can be added without altering the database structure.
2. **Embedded Documents:** Customer reviews can be stored directly within the product document as an array of embedded documents. This makes it easy to retrieve a product and all its reviews in a single query, improving performance and simplifying data modeling.
3. **Horizontal Scalability:** MongoDB is designed for horizontal scaling, allowing data to be distributed across multiple servers. This supports large catalogs and high traffic, making it suitable for growing businesses like FlexiMart.

These features make MongoDB highly adaptable for diverse, evolving product catalogs and nested data requirements.

---

## Section C: Trade-offs

While MongoDB offers flexibility, there are disadvantages compared to MySQL:

1. **Lack of ACID Transactions:** MongoDB’s support for multi-document ACID transactions is limited compared to MySQL. This can lead to data consistency issues in complex operations involving multiple collections.
2. **Weaker Relational Integrity:** MongoDB does not enforce foreign key constraints, making it harder to maintain strict relationships between entities. This can result in orphaned or inconsistent data if not managed carefully at the application level.

Choosing MongoDB involves trading off some data integrity and transactional guarantees for flexibility and scalability.