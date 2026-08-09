## Repository Overview

* **`config/pipeline_config.yaml`**  
  Centralized configuration for Kafka broker endpoints, Delta Lake paths, Qdrant parameters, and risk threshold limits.

* **`scripts/` (Pipeline Engines)**  
  * **`stream_producer.py`**: Simulates live credit card transaction events and publishes JSON streams to Kafka/Redpanda.
  * **`lakehouse_processor.py`**: Core PySpark Structured Streaming engine that handles ACID Delta Lake writes and Qdrant vector indexing.

* **`src/` (Core Framework Modules)**  
  * **`schema_validator.py`**: Pre-ETL quality checks, field presence validation, and nullability enforcement.
  * **`vector_indexer.py`**: Embeds compliance notes via HuggingFace models and upserts dense vectors into Qdrant.
  * **`flight_recorder.py`**: Centralized operational logger that tracks micro-batch execution metrics and UUID run session IDs.

* **Infrastructure & Setup**  
  * **`docker-compose.yml`**: Provisions containerized Redpanda (Kafka) broker and Qdrant Vector DB services.
  * **`requirements.txt`**: Locked PySpark, Delta Lake, Transformer, and Qdrant dependency specifications.
## Key Architecture Highlights

* **Real-Time High-Throughput Streaming:** Leverages Redpanda (Kafka API) and PySpark Structured Streaming to process high-velocity financial event streams with sub-second micro-batching.
* **Dual-Path Data Processing:** Implements a decoupled write path—persisting structured transaction records into an ACID-compliant Delta Lakehouse while routing high-risk compliance notes to vector storage.
* **Low-Latency AI Context Indexing:** Converts unstructured compliance data into 384-dimensional dense vectors using HuggingFace (`all-MiniLM-L6-v2`) and indexes them in Qdrant Vector DB for instant similarity search and RAG capabilities.
* **Enterprise-Grade Governance & Observability:** Integrates pre-ETL data quality auditing (`schema_validator.py`) and centralized execution tracking with UUID-tagged session logging (`flight_recorder.py`).
