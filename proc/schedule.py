import asyncio
import heapq
import time

from proc import Process, ProcessManager

# NOTE: Error Codes
# DEPRIORITIZED: -1


class Scheduler:
    def __init__(self, pm: ProcessManager) -> None:
        self.pm: ProcessManager = pm
        self.queue: list[tuple[int, int, int, Process, int, int]] = []
        self.seq: int = 0
        self.job_index: dict[int, tuple[int, int, int, Process, int, int]] = {}
        self.running: bool = False

    def scheduled_at(
        self,
        start_time_ms: int,
        proc: Process,
        priority: int = -1,
        repeat_count: int = 0,
        repeat_time_ms: int = 0,
    ) -> None:
        item = (
            start_time_ms,
            priority,
            self.seq,
            proc,
            repeat_count,
            repeat_time_ms,
        )
        self.seq += 1
        heapq.heappush(self.queue, item)
        self.job_index[proc.id] = item

    def scheduled_in(
        self,
        delay_ms: int,
        proc: Process,
        priority: int = -1,
        repeat_count: int = 0,
        repeat_time_ms: int = 0,
    ) -> None:
        start_time_ms = time.ticks_add(time.ticks_ms(), delay_ms)
        self.scheduled_at(start_time_ms, proc, priority, repeat_count, repeat_time_ms)

    async def run(self) -> None:
        self.running = True
        while self.running:
            if not self.queue:
                await asyncio.sleep_ms(10)
                continue

            start_time_ms, priority, seq, proc, repeat_count, repeat_time_ms = (
                self.queue[0]
            )
            now = time.ticks_ms()
            delay = time.ticks_diff(start_time_ms, now)

            if delay > 0:
                await asyncio.sleep_ms(delay)
                continue

            due = []
            now = time.ticks_ms()
            while self.queue and time.ticks_diff(self.queue[0][0], now) <= 0:
                due.append(heapq.heappop(self.queue))

            for start_time_ms, priority, seq, proc, repeat_count, repeat_time_ms in due:
                if self.job_index.get(proc.id) != (
                    start_time_ms,
                    priority,
                    seq,
                    proc,
                    repeat_count,
                    repeat_time_ms,
                ):
                    continue

                self.pm.spawn(proc)

                if repeat_count > 1:
                    next_item = (
                        time.ticks_add(start_time_ms, repeat_time_ms),
                        priority,
                        self.seq,
                        proc,
                        repeat_count - 1,
                        repeat_time_ms,
                    )
                    self.seq += 1
                    heapq.heappush(self.queue, next_item)
                    self.job_index[proc.id] = next_item
                else:
                    self.job_index.pop(proc.id, None)

            await asyncio.sleep_ms(0)

    def stop(self) -> None:
        self.running = False
        self.queue.clear()
        self.job_index.clear()

    async def shutdown(self) -> None:
        self.stop()
        await self.pm.cancel_all()

    def change_priority(self, pid: int, new_priority: int) -> None:
        item = self.job_index.get(pid)
        if item is None:
            return

        start_time_ms, _, seq, proc, repeat_count, repeat_time_ms = item
        new_item = (
            start_time_ms,
            new_priority,
            seq,
            proc,
            repeat_count,
            repeat_time_ms,
        )

        self.job_index[pid] = new_item
        heapq.heappush(self.queue, new_item)
