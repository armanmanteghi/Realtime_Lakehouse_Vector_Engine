"""
AI Vector Indexing Module
Encapsulates embedding model invocation and Qdrant upsert logic for compliance text.
"""

from typing import List, Dict, Any
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct

class VectorIndexer:
    def __init__(self, collection_name: str, model_name: str = "all-MiniLM-L6-v2", host: str = "localhost", port: int = 6333):
        self.collection_name = collection_name
        self.embedder = SentenceTransformer(model_name)
        self.vector_dim = self.embedder.get_sentence_embedding_dimension()
        self.client = QdrantClient(host=host, port=port)
        self._ensure_collection()

    def _ensure_collection(self):
        """Creates target collection in Qdrant if absent."""
        if not self.client.collection_exists(self.collection_name):
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(size=self.vector_dim, distance=Distance.COSINE)
            )

    def generate_and_upsert(self, records: List[Dict[str, Any]], text_field: str = "compliance_notes") -> int:
        """Embeds text fields and upserts vector points to Qdrant."""
        if not records:
            return 0

        points = []
        for record in records:
            text = record.get(text_field, "")
            if not text:
                continue

            vector = self.embedder.encode(text).tolist()
            point_id = abs(hash(record.get("transaction_id", str(record)))) % (10 ** 8)

            points.append(PointStruct(
                id=point_id,
                vector=vector,
                payload=record
            ))

        if points:
            self.client.upsert(collection_name=self.collection_name, points=points)

        return len(points)
