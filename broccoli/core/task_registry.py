# video_scheduler/core/task_registry.py
import logging
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)


class TaskRegistry:
    """Registry for task handlers that can be executed by workers."""

    _instance = None
    _handlers: Dict[str, Callable] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def register(self, task_type: str):
        """Decorator to register a task handler."""

        def decorator(func: Callable):
            self._handlers[task_type] = func
            logger.info(f"Registered handler for task type: {task_type}")
            return func

        return decorator

    def register_manually(self, task_type: str, handler: Callable):
        """Manually register a task handler."""
        self._handlers[task_type] = handler
        logger.info(f"Manually registered handler for task type: {task_type}")

    def get_handler(self, task_type: str) -> Optional[Callable]:
        """Get the handler for a task type."""
        return self._handlers.get(task_type)

    def get_all_handlers(self) -> Dict[str, Callable]:
        """Get all registered handlers."""
        return self._handlers

    def execute(self, task_type: str, payload: Dict[str, Any], **kwargs) -> Any:
        """Execute a task handler."""
        handler = self.get_handler(task_type)
        if not handler:
            raise ValueError(f"No handler registered for task type: {task_type}")
        return handler(payload, **kwargs)
