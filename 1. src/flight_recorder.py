"""
Audit Flight Recorder & Execution Observability Logger
Tracks unique pipeline runs, micro-batch metrics, and processing latency.
"""

import uuid
import logging
from datetime import datetime
from typing import Dict, Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class FlightRecorder:
    def __init__(self, pipeline_name: str):
        self.pipeline_name = pipeline_name
        self.execution_id = str(uuid.uuid4())
        self.start_time = datetime.utcnow()
        logging.info(f"[{self.pipeline_name}] Initiated execution session: {self.execution_id}")

    def log_micro_batch(self, batch_id: int, total_records: int, high_risk_records: int, status: str = "SUCCESS"):
        """Logs batch metrics to standard execution stream."""
        metrics = {
            "execution_id": self.execution_id,
            "pipeline": self.pipeline_name,
            "batch_id": batch_id,
            "processed_records": total_records,
            "high_risk_records": high_risk_records,
            "status": status,
            "timestamp": datetime.utcnow().isoformat()
        }
        logging.info(f"[FlightRecorder] Batch Metrics: {metrics}")
        return metrics

    def end_session(self):
        """Finalizes run and outputs total runtime performance."""
        duration = (datetime.utcnow() - self.start_time).total_seconds()
        logging.info(f"[{self.pipeline_name}] Execution {self.execution_id} finalized in {duration:.2f}s")
