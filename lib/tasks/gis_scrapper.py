from aiohttp import ClientSession
from datetime import datetime
import geopandas as gpd
import math
import time

from lib.debug import NotebookTimer
from lib.gis.defaults import HOST_SEMAPHORE_CONFIG as t_conf
from lib.gis.defaults import ENSW_DA_PROJECTION
from lib.gis.defaults import SNSW_LOT_PROJECTION
from lib.gis.defaults import SNSW_PROP_PROJECTION
from lib.gis.defaults import ENSW_ZONE_PROJECTION

from lib.gis.producer import GisProducer
from lib.service.http.exp_backoff import BackoffConfig, RetryPreference
from lib.service.http import CachedClientSession, ThrottledSession, ExpBackoffClientSession

backoff_config = BackoffConfig(RetryPreference(allowed=8))

class IPythonUi:
    def __init__(self, timer, producer):
        self.timer = None
        self.producer = None

    def on_loop(self, proj, task, page):
        import IPython.display
        import matplotlib.pyplot as plt

        IPython.display.clear_output(wait=True)
        self.timer.add({ 'items': len(page), 'count': 1 })
        self.timer.log()
        print(proj.schema.url)
        print(self.producer.progress(proj, task))
        if len(page) > 0:
            fig, ax = plt.subplots(1, 1, figsize=(2, 4))
            page.plot(ax=ax, column=proj.schema.debug_plot_column)
            plt.show()
            display(page.iloc[:1])

class ConsoleUi:
    def __init__(self, timer, producer):
        self.timer = None
        self.producer = None

    def on_loop(self, proj, task, page):
        self.timer.add({ 'items': len(page), 'count': 1 })
        self.timer.log()
        print(proj.schema.url)
        print(self.producer.progress(proj, task))

async def run(Factory):
    try:
        timer_state = { 'count': 0, 'items': 0 }
        timer = NotebookTimer('#%(count)s: %(items)s items via', timer_state)
        # This http client composes a cache layer and a throttling
        # layer. Some GIS serves, such as the ones below may end
        # up blocking you if you perform to much traffic at once.
        #
        #   - Caching ensure requests that do not need to be
        #     repeated, are not repeated.
        #
        #   - Expotenial backoff is here to retry when in the
        #     the event any requests to the GIS server fail.
        #
        #   - Throttling of course ensure we do not have more
        #     active requests to one host at a time. The max
        #     active requests is set on a host basis.
        #
        throttle_s = ThrottledSession.create(t_conf)
        ebackoff_s = ExpBackoffClientSession.create(backoff_config, throttle_s)

        async with CachedClientSession.create(ebackoff_s) as session:
            producer = GisProducer(session)
            producer.queue(SNSW_PROP_PROJECTION)
            producer.queue(SNSW_LOT_PROJECTION)
            # reader.queue(ENSW_ZONE_PROJECTION)
            # reader.queue(ENSW_DA_PROJECTION)

            ui = Factory(timer, producer)
            async for proj, task, page in producer.produce([]):
                ui.on_loop(proj, task, page)

            print(f"finished loading GIS'")
    except Exception as e:
        raise e

async def run_in_notebook():
    await run(IPythonUi)

if __name__ == '__main__':
    import asyncio
    import resource
    import psutil

    process = psutil.Process()
    slim, hlim = resource.getrlimit(resource.RLIMIT_NOFILE)

    print(process.num_fds(), (slim, hlim))
    asyncio.run(run(ConsoleUi))
