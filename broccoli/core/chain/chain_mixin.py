from typing import Any

from broccoli.core.task.task import Task


class ChainWorkerMixin:
    """Mixin to add chain support to a worker."""

    def post_process(self, task: Task, success: bool) -> None:
        """Override this in your worker and call super().post_process()."""
        chain_id = task.payload.get("__chain_id")

        if chain_id:
            # Delete the chain-prefixed task key now that it's done
            self._redis.delete(f"chain:{task.task_id}")

            # Completion tasks have __chain_id set (so they get cleaned up above)
            # but must not call continue_chain — they are not sequential chain steps
            is_completion_task = task.payload.get("__chain_position") is None
            if is_completion_task:
                return

            if success:
                from broccoli.core.chain.task_chain import TaskChain

                chain = TaskChain()
                # Pass push_completion_task=False so continue_chain does not also
                # enqueue the completion task — on_finish handles that directly.
                # Pushing it AND calling on_finish would run on_chain_finished twice.
                finished = chain.continue_chain(
                    task, task.result, push_completion_task=True
                )
                if finished:
                    self.on_finish(chain_id, task.result)

    def on_finish(self, chain_id: str, final_result: Any) -> None:
        """Hook called when the entire chain is finished. Override in your worker."""
        pass  # Your existing on_finish logic here
