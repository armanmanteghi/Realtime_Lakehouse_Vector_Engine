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
