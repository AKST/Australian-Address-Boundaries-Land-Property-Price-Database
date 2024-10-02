import csv
from collections import defaultdict, deque
import concurrent.futures
import glob
import logging
import os
from threading import Lock
from typing import List, Dict, Set, Any

from lib.service.database import DatabaseService

from .discovery import GnafPublicationTarget
from .scheduler import GnafDataIngestionScheduler

_schema = 'gnaf'
_WORKER_COUNT: int = os.cpu_count() or 1
_BATCH_SIZE: float = 64000 / _WORKER_COUNT # idk what I'm doing here tbh.

_logger = logging.getLogger(__name__)

def ingest(target: GnafPublicationTarget, db: DatabaseService):
    scheduler = GnafDataIngestionScheduler.create(target)

    with concurrent.futures.ThreadPoolExecutor(max_workers=_WORKER_COUNT) as executor:
        futures = [executor.submit(worker, scheduler, db) for _ in range(_WORKER_COUNT)]
        for future in concurrent.futures.as_completed(futures):
            future.result()

def worker(schedule: GnafDataIngestionScheduler, db: DatabaseService):
    for file in schedule.iter():
        table_name = _get_table_name(file)

        with db.connect() as conn, conn.cursor() as cursor:
            with open(file, 'r') as f:
                _logger.info(f"Populating from {os.path.basename(file)}")
                reader = csv.reader(f, delimiter='|')
                headers = next(reader)
                insert_query = f"""
                    INSERT INTO {_schema}.{table_name} ({', '.join(headers)})
                    VALUES ({', '.join(['%s'] * len(headers))})
                    ON CONFLICT DO NOTHING
                """

                for batch_index, batch in enumerate(_get_batches(_BATCH_SIZE, reader)):
                    try:
                        cursor.executemany(insert_query, batch)
                    except Exception as e:
                        print(f"Error inserting batch {batch_index + 1} into {table_name}: {e}")
                        raise e
                conn.commit()

def _get_table_name(file):
    file = os.path.splitext(os.path.basename(file))[0]
    sidx = 15 if file.startswith('Authority_Code') else file.find('_')+1
    return file[sidx:file.rfind('_')]

def _get_batches(batch_size, reader):
    batch = []
    for row in reader:
        row = [(None if v == "" else v) for v in (v.strip() for v in row)]
        batch.append(row)

        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch
