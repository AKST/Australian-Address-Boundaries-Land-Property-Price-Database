import pytest
import asyncio

from ..partition_lock import PartitionLock

@pytest.mark.asyncio
async def test_concurrent_entry_access():
    """Test that entry operations can run concurrently within the same partition."""
    lock = PartitionLock()
    results = []

    async def entry_task(partition_id: str, idx: int):
        async with lock.entry_access(partition_id):
            results.append(f"entry-{partition_id}-{idx}")
            await asyncio.sleep(0.1)

    await asyncio.gather(
        entry_task("A", 1),
        entry_task("A", 2),
        entry_task("A", 3),
    )

    assert sorted(results) == ["entry-A-1", "entry-A-2", "entry-A-3"]

@pytest.mark.asyncio
async def test_partition_access_blocks_entry():
    """Test that a whole-of-partition operation blocks entry operations in the same partition."""
    lock = PartitionLock()
    results = []

    async def entry_task(partition_id: str):
        async with lock.entry_access(partition_id):
            results.append(f"entry-{partition_id}")
            await asyncio.sleep(0.2)

    async def partition_task(partition_id: str):
        async with lock.whole_partition_access(partition_id):
            results.append(f"whole-{partition_id}")
            await asyncio.sleep(0.2)

    await asyncio.gather(
        entry_task("A"),
        partition_task("A"),
        entry_task("A"),
    )

    # The whole partition task should happen after the first entry task but block subsequent ones
    assert results == ["entry-A", "whole-A", "entry-A"]

@pytest.mark.asyncio
async def test_partition_independence():
    """Test that operations in different partitions do not block each other."""
    lock = PartitionLock()
    results = []

    async def entry_task(partition_id: str, idx: int):
        async with lock.entry_access(partition_id):
            results.append(f"entry-{partition_id}-{idx}")
            await asyncio.sleep(0.1)

    async def partition_task(partition_id: str):
        async with lock.whole_partition_access(partition_id):
            results.append(f"whole-{partition_id}")
            await asyncio.sleep(0.2)

    await asyncio.gather(
        entry_task("A", 1),
        partition_task("B"),
        entry_task("A", 2),
        entry_task("B", 1),
    )

    # Entries in partition "A" and partition "B" should not block each other
    assert "whole-B" in results
    assert results.index("whole-B") < results.index("entry-B-1")

@pytest.mark.asyncio
async def test_reentrant_entry_access():
    """Test that reentrant entry access to the same partition works correctly."""
    lock = PartitionLock()
    results = []

    async def reentrant_task(partition_id: str):
        async with lock.entry_access(partition_id):
            results.append(f"entry-{partition_id}-1")
            async with lock.entry_access(partition_id):
                results.append(f"entry-{partition_id}-2")
                await asyncio.sleep(0.1)

    await reentrant_task("A")
    assert results == ["entry-A-1", "entry-A-2"]

@pytest.mark.asyncio
async def test_sequential_partition_access():
    """Test that multiple whole-of-partition operations run sequentially."""
    lock = PartitionLock()
    results = []

    async def partition_task(partition_id: str, idx: int):
        async with lock.whole_partition_access(partition_id):
            results.append(f"whole-{partition_id}-{idx}")
            await asyncio.sleep(0.2)

    await asyncio.gather(
        partition_task("A", 1),
        partition_task("A", 2),
    )

    assert results == ["whole-A-1", "whole-A-2"]

@pytest.mark.asyncio
async def test_concurrent_entries_across_partitions():
    """Test that entry operations across multiple partitions run concurrently."""
    lock = PartitionLock()
    results = []

    async def entry_task(partition_id: str, idx: int):
        async with lock.entry_access(partition_id):
            results.append(f"entry-{partition_id}-{idx}")
            await asyncio.sleep(0.1)

    await asyncio.gather(
        entry_task("A", 1),
        entry_task("B", 1),
        entry_task("A", 2),
        entry_task("B", 2),
    )

    assert sorted(results) == ["entry-A-1", "entry-A-2", "entry-B-1", "entry-B-2"]

@pytest.mark.asyncio
async def test_partition_independence_with_whole_partition():
    """Test that a whole-of-partition operation in one partition does not block entry operations in another partition."""
    lock = PartitionLock()
    results = []

    async def entry_task(partition_id: str, idx: int):
        async with lock.entry_access(partition_id):
            results.append(f"entry-{partition_id}-{idx}")
            await asyncio.sleep(0.1)

    async def partition_task(partition_id: str):
        async with lock.whole_partition_access(partition_id):
            results.append(f"whole-{partition_id}")
            await asyncio.sleep(0.3)

    await asyncio.gather(
        entry_task("A", 1),
        partition_task("B"),
        entry_task("A", 2),
        entry_task("C", 1),
    )

    # Whole-of-partition "B" should not block entry operations in "A" and "C"
    assert "whole-B" in results
    assert results.index("whole-B") < results.index("entry-C-1")

