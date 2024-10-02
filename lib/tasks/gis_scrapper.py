from datetime import datetime
import geopandas as gpd
from logging import getLogger
import math
import time
from typing import Self

from lib.pipeline.gis.defaults import HOST_SEMAPHORE_CONFIG, BACKOFF_CONFIG
from lib.pipeline.gis.defaults import ENSW_DA_PROJECTION
from lib.pipeline.gis.defaults import SNSW_LOT_PROJECTION
from lib.pipeline.gis.defaults import SNSW_PROP_PROJECTION
from lib.pipeline.gis.defaults import ENSW_ZONE_PROJECTION
from lib.pipeline.gis.producer import GisProducer
from lib.pipeline.gis.predicate.date import YearMonth, DateRangeParam
from lib.service.io import IoService
from lib.service.clock import ClockService
from lib.service.http import CachedClientSession, ThrottledClientSession, ExpBackoffClientSession
from lib.service.http.middleware.cache import FileCacher
from lib.service.http.middleware.exp_backoff import BackoffConfig, RetryPreference

backoff_config = BackoffConfig(RetryPreference(allowed=8))

class NotebookTimer:
    def __init__(self, message, state):
        self._message = message
        self._start = time.time()
        self._state = state

    def add(self, state):
        for k, v in state.items():
            self._state[k] += v

    def get_message(self):
        time_of_day = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        t = int(time.time() - self._start)
        th, tm, ts = t // 3600, t // 60 % 60, t % 60
        msg = self._message % self._state
        return f'{time_of_day} {msg} @ {th}h {tm}m {ts}s'

class IPythonUi:
    timer: NotebookTimer

    def __init__(self: Self, timer, producer):
        self.timer = timer
        self.producer = producer

    def on_loop(self: Self, proj, task, page):
        from IPython.display import clear_output, display # type: ignore
        import matplotlib.pyplot as plt

        clear_output(wait=True)
        self.timer.add({ 'items': len(page), 'count': 1 })
        print(self.timer.get_message())
        print(proj.schema.url)
        print(self.producer.progress(proj, task))
        if len(page) > 0:
            fig, ax = plt.subplots(1, 1, figsize=(2, 4))
            page.plot(ax=ax, column=proj.schema.debug_plot_column)
            plt.show()
            display(page.iloc[:1])

    def log(self: Self, m: str):
        print(m)

class ConsoleUi:
    timer: NotebookTimer
    _logger = getLogger(f'{__name__}.ConsoleUi')

    def __init__(self: Self, timer, producer):
        self.timer = timer
        self.producer = producer

    def on_loop(self: Self, proj, task, page):
        self.timer.add({ 'items': len(page), 'count': 1 })
        self._logger.info(self.timer.get_message())
        self._logger.info(proj.schema.url)
        self._logger.info(self.producer.progress(proj, task))

    def log(self: Self, m: str):
        self._logger.info(m)

def initialise_session(open_file_limit):
    io_service = IoService.create(open_file_limit)

    return CachedClientSession.create(
        session=ExpBackoffClientSession.create(
            session=ThrottledClientSession.create(HOST_SEMAPHORE_CONFIG),
            config=BACKOFF_CONFIG,
        ),
        file_cache=FileCacher.create(io=io_service),
        io_service=io_service,
    )

async def run(UiFactory, open_file_limit):
    """
    The http client composes a cache layer and a throttling
    layer. Some GIS serves, such as the ones below may end
    up blocking you if you perform to much traffic at once.

      - Caching ensure requests that do not need to be
        repeated, are not repeated.

      - Expotenial backoff is here to retry when in the
        the event any requests to the GIS server fail.

      - Throttling of course ensure we do not have more
        active requests to one host at a time. The max
        active requests is set on a host basis.
    """
    clock = ClockService()
    # param = [DateRangeParam(YearMonth(2023, 1), YearMonth(2023, 2), clock)]

    param = []
    try:
        timer_state = { 'count': 0, 'items': 0 }
        timer = NotebookTimer('#%(count)s: %(items)s items via', timer_state)

        async with initialise_session(open_file_limit) as session:
            producer = GisProducer(session)
            producer.queue(SNSW_PROP_PROJECTION)
            producer.queue(SNSW_LOT_PROJECTION)
            # reader.queue(ENSW_ZONE_PROJECTION)
            # reader.queue(ENSW_DA_PROJECTION)

            ui = UiFactory(timer, producer)
            async for proj, task, page in producer.produce(param):
                ui.on_loop(proj, task, page)
            ui.log('finished loading GIS')
    except Exception as e:
        raise e

async def run_in_notebook():
    await run(IPythonUi, None)

if __name__ == '__main__':
    import resource
    import logging
    import asyncio

    slim, hlim = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(slim * 0.8)

    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    try:
        asyncio.run(run(ConsoleUi, file_limit))
    except ExceptionGroup as e:
        import psutil

        process = psutil.Process()

        print(f'io open files {process.num_fds()}, limits {(slim, hlim)}')
        raise e
