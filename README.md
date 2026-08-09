Implements the core ETL engine (lakehouse_processor.py) for real-time stream consumption and dual-path storage:

- Consumes JSON financial transaction streams from Kafka/Redpanda
- Enforces a strictly typed Spark schema across incoming event payloads
- Persists micro-batches into local Delta Lake tables for ACID transactional storage
- Filters high-risk events (risk_score > 0.70) to generate 384-dimensional embeddings via SentenceTransformers
- Upserts vector records and metadata into Qdrant DB for downstream RAG and fraud compliance search
