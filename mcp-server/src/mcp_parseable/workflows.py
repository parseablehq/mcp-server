from enum import Enum
from dataclasses import dataclass, field
import asyncio
import logging
from typing import Optional, Callable, Awaitable, List
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class WorkflowResult:
    timestamp: datetime
    message: str

class WorkflowType(str, Enum):
    READ_LATENCY = "read_query_latency"
    WRITE_LATENCY = "write_query_latency"
    STORAGE_GROWTH = "storage_growth"

@dataclass
class Workflow:
    type: WorkflowType
    description: str
    monitor_func: Callable[[], Awaitable[str]]
    is_running: bool = False
    task: Optional[asyncio.Task] = None
    # Store last N results (default 100)
    max_results: int = 100
    results: List[WorkflowResult] = field(default_factory=list)

    async def start_monitoring(self, interval_seconds: int = 10):
        """Start continuous monitoring with the specified interval"""
        if self.is_running:
            return "Workflow is already running"

        async def monitor_loop():
            while True:
                try:
                    result = await self.monitor_func()
                    workflow_result = WorkflowResult(
                        timestamp=datetime.utcnow(),
                        message=result
                    )
                    self.results.append(workflow_result)
                    # Keep only last N results
                    if len(self.results) > self.max_results:
                        self.results = self.results[-self.max_results:]
                    logger.info(f"Workflow {self.type} result: {result}")
                except Exception as e:
                    logger.error(f"Error in workflow {self.type}: {e}")
                await asyncio.sleep(interval_seconds)

        self.task = asyncio.create_task(monitor_loop())
        self.is_running = True
        return f"Started monitoring workflow: {self.type}"

    def stop_monitoring(self) -> str:
        """Stop the monitoring loop"""
        if not self.is_running:
            return "Workflow is not running"
        
        if self.task:
            self.task.cancel()
        self.is_running = False
        return f"Stopped monitoring workflow: {self.type}"

    def get_recent_results(self, limit: int = 5) -> List[WorkflowResult]:
        """Get the N most recent results"""
        return self.results[-limit:]