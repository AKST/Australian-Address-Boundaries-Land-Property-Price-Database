import asyncio
from typing import Any, Dict, Self


class EntryAccess:
    """Context manager for entry access to a specific partition."""
    def __init__(self: Self, lock: 'PartitionLock', partition_id: str):
        self._lock = lock
        self._partition_id = partition_id

    async def __aenter__(self: Self) -> 'EntryAccess':
        await self._lock.acquire_entry_access(self._partition_id)
        return self

    async def __aexit__(self: Self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        await self._lock.release_entry_access(self._partition_id)

class WholePartitionAccess:
    """Context manager for whole-of-partition access to a specific partition."""
    def __init__(self: Self, lock: 'PartitionLock', partition_id: str):
        self._lock = lock
        self._partition_id = partition_id

    async def __aenter__(self: Self) -> 'WholePartitionAccess':
        await self._lock.acquire_whole_partition_access(self._partition_id)
        return self

    async def __aexit__(self: Self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        await self._lock.release_whole_partition_access(self._partition_id)

def _inc_key(d: Dict[str, int], index: str):
    if index not in d:
        d[index] = 0
    d[index] += 1

class PartitionLock:
    """
    A lock allowing concurrent entry operations within and across partitions.
    Whole-of-partition operations require exclusive access to their partition but do
    not block operations in other partitions.
    """
    _partition_entry_counts: Dict[str, int]
    _partition_exclusive: Dict[str, bool]
    _partition_waiting: Dict[str, int]

    def __init__(self: Self):
        self._condition = asyncio.Condition()
        self._partition_entry_counts = {}
        self._partition_exclusive = {}
        self._partition_waiting = {}

    def _cannot_gain_access_entry(self: Self, partition_id: str) -> bool:
        return self._partition_exclusive.get(partition_id, False) \
            or self._partition_waiting.get(partition_id, 0) > 0

    def _cannot_access_partition(self: Self, partition_id: str) -> bool:
        return self._partition_entry_counts.get(partition_id, 0) > 0 \
            or self._partition_exclusive.get(partition_id, False)

    async def acquire_entry_access(self: Self, partition_id: str):
        """
        Acquire entry access for a specific partition. Waits if the partition
        is under whole-of-partition access.
        """
        async with self._condition:
            while self._cannot_gain_access_entry(partition_id):
                await self._condition.wait()
            _inc_key(self._partition_entry_counts, partition_id)

    async def release_entry_access(self, partition_id: str):
        """
        Release entry access for a specific partition. If no more entry operations
        are active, notify other waiting tasks.
        """
        async with self._condition:
            self._partition_entry_counts[partition_id] -= 1
            self._condition.notify_all()

    async def acquire_whole_partition_access(self, partition_id: str):
        """
        Acquire whole-of-partition access for a specific partition. Waits until all
        entry operations for the partition are completed.
        """
        async with self._condition:
            _inc_key(self._partition_waiting, partition_id)

            while self._cannot_access_partition(partition_id):
                await self._condition.wait()

            self._partition_waiting[partition_id] -= 1
            self._partition_exclusive[partition_id] = True

    async def release_whole_partition_access(self, partition_id: str):
        """
        Release whole-of-partition access for a specific partition, allowing new
        entry operations to proceed.
        """
        async with self._condition:
            self._partition_exclusive[partition_id] = False
            self._condition.notify_all()

    def entry_access(self, partition_id: str) -> EntryAccess:
        """Return a context manager for entry access to a specific partition."""
        return EntryAccess(self, partition_id)

    def whole_partition_access(self, partition_id: str) -> WholePartitionAccess:
        """Return a context manager for whole-of-partition access to a specific partition."""
        return WholePartitionAccess(self, partition_id)

class VoidPartitionLock(PartitionLock):
    async def acquire_entry_access(self: Self, partition_id: str):
        pass

    async def release_entry_access(self, partition_id: str):
        pass

    async def acquire_whole_partition_access(self, partition_id: str):
        pass

    async def release_whole_partition_access(self, partition_id: str):
        pass

