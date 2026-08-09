import asyncio
import gc
import time
import micropython

# NOTE: Implement
# Preloading methods
# Pre-allocating memory
# All local variables

# NOTE: Error codes
# UNREGISTERED: 0
# FAILURE: -1


class Process:
    @micropython.native
    def __init__(
        self,
        coroutine: callable,
        name: str,
        max_restarts: int = 0,
    ) -> None:
        self.id: int = 0
        self.name: str = name
        self.coroutine: callable = coroutine
        self.task: asyncio.Task[None] | None = None
        self.created_at: float = time.time()
        self.running: bool = False
        self.restart_count: int = 0
        self.max_restarts: int = max_restarts

    async def run(self) -> None:
        self.running = True
        coroutine = self.coroutine
        max_restarts = self.max_restarts
        try:
            while True:
                try:
                    await coroutine()
                    break
                except asyncio.CancelledError:
                    raise
                except Exception:
                    restart_count = self.restart_count
                    if restart_count >= max_restarts:
                        break
                    self.restart_count = restart_count + 1
                    await asyncio.sleep_ms(0)

        finally:
            self.running = False
    async def kill(self) -> None:
        task = self.task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self.running = False


class ProcessManager:
    @micropython.native
    def __init__(
        self,
        max_process: int = 5,
        min_free_memory: int = 2048,
    ) -> None:
        self.processes: dict[int, Process] = {}
        self.next_id: int = 1
        self.max_process: int = max_process
        self.min_free_memory: int = min_free_memory

    @micropython.native
    def _can_spawn(self) -> bool:
        processes = self.processes
        if len(processes) >= self.max_process:
            return False
        return gc.mem_free() >= self.min_free_memory

    async def spawn(self, proc: Process) -> int:
        processes = self.processes
        if len(processes) >= self.max_process:
            return FAILURE
        if gc.mem_free() < self.min_free_memory:
            return FAILURE
        pid = self.next_id
        proc.id = pid
        self.next_id = pid + 1
        processes[pid] = proc
        proc.task = asyncio.create_task(proc.run())
        return pid

    async def kill(self, pid: int) -> bool:
        processes = self.processes
        proc = processes.get(pid)
        if proc is None:
            return False
        task = proc.task

        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        proc.running = False
        del processes[pid]
        return True

    async def cancel_all(self) -> None:
        processes = self.processes
        if not processes:
            return
        for proc in processes.values():
            task = proc.task

            if task is not None and not task.done():
                task.cancel()
        await asyncio.sleep_ms(0)
        processes.clear()

    # async def status(self) -> None:
    #     for pid, proc in self.processes.items():
    #         state = "Running" if proc.running else "IDLE/Done"
    #         await safe_print(f"ID: {pid}\tNAME: {proc.name}\tState: {state}")
