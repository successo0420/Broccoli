from typing import Any

from broccoli.core.task.task import Task


class ChainWorkerMixin:
    """Mixin to add chain support to a worker."""

    def post_process(self, task: Task, success: bool) -> None:
        """Override this in your worker and call super().post_process()."""
        # Check if this task is part of a chain
        chain_id = task.payload.get("__chain_id")
        if chain_id and success:
            from broccoli.core.chain.task_chain import TaskChain

            chain = TaskChain()
            finished = chain.continue_chain(task, task.result)
            if finished:
                self.on_finish(chain_id, task.result)

    def on_finish(self, chain_id: str, final_result: Any) -> None:
        """Hook called when the entire chain is finished. Override in your worker."""
        pass  # Your existing on_finish logic here
