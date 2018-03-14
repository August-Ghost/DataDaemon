# -*- coding: utf-8 -*-
from os import path, makedirs
from asyncio import CancelledError, sleep, ensure_future
from ..utilities import glob, dump_exception
from datetime import datetime, timedelta


class BaseGrabber:

    def __init__(self, loop=None):

        self.loop = loop if loop else glob.CURRENT_EVENTLOOP
        self.ratelimit = 0
        self.max_retry = 7
        self.ratelimit_strategy = "smooth"
        self.exchange = None
        self.tradepairs = None
        self.query = {}
        self.dumploc = "./dump-{name}".format(name=self.__class__.__name__)
        self.run_period = 0
        self.retry_times = 0
        self._query_counter = 0
        self._ratelimit_window = None
        self.total = 0
        self._start_time = 0
        self.last_query_time = 0
        self.quit = False
        self.awake = True

        # Apply global grabber config from
        self.__dict__.update(glob.CONFIG.grabber_glob_config)
        self.__dict__.update(self.config())
        if not path.exists(self.dumploc):
            makedirs(self.dumploc)
        self._grab_task = None

    @property
    def is_periodic(self):
        return True if self.run_period > 0 else False

    def config(self):
        return {
            "run_period": 0,
            "exchange": None,
            "tradepairs": None,
            "query":{
                "timeframe": None,
                "limit":500,
                "since":None,
                "params":{}
            },
            "dumploc": None,
        }

    def __call__(self, *args, **kwargs):
        if not self._grab_task:
            self._grab_task = ensure_future(self.fetch(), loop=self.loop)

    def start(self):
        try:
            if not self._grab_task:
                self._grab_task = ensure_future(self.fetch(), loop=self.loop)
        except Exception as e:
            print(e)

    def stop(self):
        self.quit = True
        if not self.awake:
            if self._grab_task:
                self._grab_task.cancel()

    def kill(self):
        if self._grab_task:
            self._grab_task.cancel()

    async def sleep(self, delay, *args, **kargs):
        self.awake = False
        try:
            await sleep(delay, *args, **kargs)
        except CancelledError:
            pass
        finally:
            self.awake = True

    async def fetch(self):
        glob.SYSLOGGER.info("{0} init. Save data at {1}".format(self.__class__.__name__, self.dumploc))
        await self.on_grabber_init()
        try:
            while not self.quit:
                glob.SYSLOGGER.info("{0} task start.".format(self.__class__.__name__))
                self._start_time = self.loop.time()
                await self.on_task_start()
                await self.grab_logic()
                glob.SYSLOGGER.info("{0} finished.".format(self.__class__.__name__))
                await self.schedule()
        except CancelledError:
            await self.on_grabber_quit()
        except Exception as e:
            exception_dumpfile, brief_info = dump_exception(e, self.dumploc)
            msg = "{0} failed. Traceback info dump to: {1}.\nBrief info:\n{2}"
            glob.ERRLOGGER.error(msg.format(self.__class__.__name__,
                                            exception_dumpfile,brief_info))
            await self.on_grabber_failure(e)
            glob.CURRENT_EVENTLOOP.call_exception_handler({"exception":e,
                                                           "prototype":self.__class__})
        finally:
            glob.SYSLOGGER.info("{0} quit.".format(self.__class__.__name__))


    async def grab_logic(self):
        raise NotImplemented

    async def schedule(self):
        if self.run_period:
            delay = self.run_period - (self.loop.time() - self._start_time)
            if delay > 0:
                msg = "Schedule next turn of {0} at {1}"
                next_turn = (datetime.now() + timedelta(seconds=delay)).strftime("%Y-%m-%d %H:%M:%S")
                glob.SYSLOGGER.info(msg.format(self.__class__.__name__, next_turn))
                await self.sleep(delay)

    async def deal_with_current_tradepair(self, pair):
        raise NotImplemented

    async def query_rate_control(self):
        if self.ratelimit > 0:
            self._query_counter += 1

            if self._ratelimit_window:
                now = self.loop.time()
                window_start, window_end = self._ratelimit_window

                if now <= window_end:
                    if self.ratelimit_strategy == "burst":
                        if self._query_counter > self.ratelimit:
                            await self.sleep(window_end - now + 0.01)
                            self._ratelimit_window = None
                    else:
                        # delay = 60 / (self.ratelimit - 1) - (self.loop.time() - self.last_query_time)
                        delay = 60 / (self.ratelimit - 1)
                        await self.sleep(delay if delay > 0 else 0)
                        self._query_counter = 1
            now = self.loop.time()
            self._ratelimit_window = (now, now + 60)

        # self.last_query_time = self.loop.time()

    async def on_grabber_init(self):
        pass

    async def on_task_start(self):
        pass

    async def before_tradepair_start(self, pair):
        pass

    async def fetch_data_from_exchange(self, pair, since):
        raise NotImplemented

    async def on_query_succeed(self, pair, data):
        pass

    def parseData(self, pair, data):
        pass

    def on_tradepair_done(self, pair):
        pass

    def on_task_done(self):
        pass

    async def on_grabber_failure(self, error):
        pass

    async def on_grabber_quit(self):
        pass

    async def handle_exception(self, e, pair, current_chunk_id):
        raise NotImplemented

    async def handle_unexpected_exception(self, e, pair, current_chunk_id):
        pass