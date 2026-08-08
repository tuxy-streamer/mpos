import asyncio
import gc
import time

# NOTE: Implement
# Preloading methods
# Pre-allocating memory
# All local variables

# NOTE: Error codes
# UNREGISTERED: 0
# FAILURE: -1


print_lock = asyncio.Lock()


async def safe_print(*args: object) -> None:
    async with print_lock:
        print(*args)


class Process:
    def __init__(self, coroutine: callable, name: str, max_restarts: int = 0) -> None:
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
        while True:
            try:
                await self.coroutine()
                break
            except asyncio.CancelledError:
                self.running = False
                raise
            except Exception as e:
                if self.restart_count < self.max_restarts:
                    self.restart_count += 1
                    await safe_print(
                        f"Restarting [{self.name}] "
                        + f"({self.restart_count}/{self.max_restarts}): {e}"
                    )
                    await asyncio.sleep_ms(10)
                else:
                    await safe_print(f"[{self.name}] failed permanently.")
                    self.running = False
                    break

    async def kill(self) -> None:
        task = self.task
        if task is not None and not task.done():
            _ = task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self.running = False


class ProcessManager:
    def __init__(self, min_free_memory: int = 2048) -> None:
        self.processes: dict[int, Process] = {}
        self.next_id: int = 1
        self.min_free_memory: int = min_free_memory

    async def check_memory_threshold(self) -> bool:
        free: int = gc.mem_free()
        if free < self.min_free_memory:
            gc.collect()
            return gc.mem_free() >= self.min_free_memory
        return True

    async def spawn(self, proc: Process) -> int:
        if not await self.check_memory_threshold():
            await safe_print(f"Spawn blocked [{proc.name}]")
            return -1

        proc.id = self.next_id
        proc.task = asyncio.create_task(proc.run())
        self.processes[proc.id] = proc
        self.next_id += 1
        await safe_print(f"Started [{proc.name}] (ID: {proc.id})")
        return proc.id

    async def kill(self, pid: int) -> bool:
        proc = self.processes.get(pid)
        if proc is None:
            return False
        await proc.kill()
        del self.processes[pid]
        await safe_print(f"Killed [{proc.name}]")
        return True

    async def cancel_all(self) -> None:
        for proc in self.processes.values():
            if proc.task:
                _ = proc.task.cancel()
        self.processes.clear()

    async def status(self) -> None:
        await safe_print(f"Active Processes = ({len(self.processes)})")
        for pid, proc in self.processes.items():
            state = "Running" if proc.running else "IDLE/Done"
            await safe_print(f"ID: {pid}\tNAME: {proc.name}\tState: {state}")
