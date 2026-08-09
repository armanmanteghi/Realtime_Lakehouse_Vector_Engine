Realtime_Lakehouse_Vector_Engine/
│
├── config/
│   └── pipeline_config.yaml      # Centralized pipeline configuration settings
├── scripts/
│   ├── stream_producer.py        # Real-time Kafka/Redpanda event stream generator
│   └── lakehouse_processor.py    # PySpark Structured Streaming core driver engine
├── src/
│   ├── __init__.py               # Python package initialization marker
│   ├── schema_validator.py       # Pre-ETL schema auditing & nullability validator
│   ├── vector_indexer.py         # HuggingFace vector embedding & Qdrant upsert engine
│   └── flight_recorder.py        # Centralized operational audit & execution logger
├── docker-compose.yml            # Local container orchestration (Redpanda + Qdrant)
├── requirements.txt              # Core project dependencies & library specifications
└── README.md                     # System architecture & file documentation
