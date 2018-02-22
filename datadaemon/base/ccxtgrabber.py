# -*- coding: utf-8 -*-
from .basegrabber import BaseGrabber
from os import path, makedirs, replace
from sys import exc_info
import pickle
from asyncio import CancelledError, sleep, ensure_future
from ..utilities import Finished, RetryLatest, Break, Abandon, Terminate
from ..utilities import glob, dump_exception, exec_brief_info
from ccxt import BaseError as CcxtBaseError
from aiohttp.client_exceptions import ClientError
from collections import defaultdict, deque
from datetime import datetime, timedelta


class CCXTGrabber(BaseGrabber):

    def __init__(self, loop=glob.CURRENT_EVENTLOOP):
        super(CCXTGrabber, self).__init__(loop=loop)
        self._validated_tradpairs = []
        self._status = defaultdict(int)
        self.status_file = path.join(self.dumploc, "{name}_status.pickle".format(name=self.__class__.__name__))

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

    @property
    def status(self):
        if (not self._status):
            if path.exists(self.status_file):
                with open(self.status_file, "rb") as status_f:
                    self._status = pickle.load(status_f)
        return self._status

    async def grab_logic(self):
        for pair in self._validated_tradpairs:
            await self.before_tradepair_start(pair)

        queue = deque(self._validated_tradpairs)
        while queue and not self.quit:
            pair = queue.popleft()
            try:
                await self.deal_with_current_tradepair(pair)
            except Finished:
                await self.loop.run_in_executor(glob.PROCESSPOOLEXEC,
                                                             self.on_tradepair_done, pair)
            except Abandon:
                break
            except Terminate:
                self.quit = True
                raise CancelledError
            except Exception as e:
                raise e
            else:
                queue.append(pair)
        else:
            if not self.quit:
                await self.loop.run_in_executor(glob.PROCESSPOOLEXEC,
                                                self.on_task_done)
            else:
                raise CancelledError

    async def load_markets(self, auto_retry=True, max_retry=3):
        trails = 0
        err = None
        while trails < max_retry and not self.quit:
            try:
                await self.exchange.load_markets()
            except CcxtBaseError as e:
                err = e
                if auto_retry:
                    await self.sleep(5 << trails)
                    trails += 1
                else:
                    raise e
            else:
                break
        else:
            raise err

    async def get_validated_tradepairs(self, max_trial=3):
        glob.SYSLOGGER.info("{0} validating tradepairs.".format(self.__class__.__name__))
        validated = []
        for pair in self.tradepairs:
            if not self.quit:
                trials = 0
                glob.ACCESSLOGGER.info("Validating:  {0} -> {1}.".format(self.__class__.__name__, pair))
                while trials < max_trial and not self.quit:
                    try:
                        await self.query_rate_control()
                        next_chunk, data = await self.fetch_data_from_exchange(pair, 0)
                    except Exception:
                        await self.sleep(5 << trials)
                        trials += 1
                    else:
                        validated.append(pair)
                        break
                else:
                    glob.SYSLOGGER.warn("Invalid tradepair: {0} -> {1}.".format(self.__class__.__name__, pair))
            else:
                break
        return validated

    async def deal_with_current_tradepair(self, pair):
        # Fault tolerance
        turns = 0
        while turns < 2:
            try:
                await self.query_rate_control()
                next_chunk, data = await self.fetch_data_from_exchange(pair, self.status[pair])
            except Exception as e:
                msg = "An exception occurred in {0}. Brief info:\n{1}"
                glob.SYSLOGGER.warn(msg.format(self.__class__.__name__, exec_brief_info(e)),
                                    exc_info=exc_info())
                try:
                    await self.handle_exception(e, pair, self.status[pair])
                # Retry latest chunk
                except RetryLatest:
                    msg = "{0} retry {1} -> {2}."
                    glob.SYSLOGGER.info(msg.format(self.__class__.__name__, pair, self.status[pair]))
                    self.retry_times += 1
                    continue
                # Drop current tradepair
                except Break:
                    msg = "{0} drop {1} -> {2}. Try next tradepair."
                    glob.SYSLOGGER.info(msg.format(self.__class__.__name__, pair, self.status[pair]))
                    self.retry_times = 0
                    break
                except Abandon:
                    msg = "{0} abandoned current task."
                    glob.SYSLOGGER.info(msg.format(self.__class__.__name__))
                    self.retry_times = 0
                    raise Abandon
                except Terminate:
                    msg = "{0} terminated."
                    glob.SYSLOGGER.info(msg.format(self.__class__.__name__))
                    self.retry_times = 0
                    raise Terminate
                except Exception as e:
                    msg = "Exception not absolved. {0} will be terminated."
                    glob.SYSLOGGER.info(msg.format(self.__class__.__name__))
                    await self.handle_unexpected_exception(e, pair, self.status[pair])
                    self.retry_times = 0
                    raise e
            else:
                msg = "{0} -> {1} -> {2}"
                glob.ACCESSLOGGER.info(msg.format(self.__class__.__name__,
                                                  pair,
                                                  self.status[pair],))
                self.retry_times = 0
                await self.on_query_succeed(pair, data)
                if data:
                    # Parse data
                    try:
                        await self.loop.run_in_executor(glob.THREADPOOLEXEC,
                                                                     self.parseData, pair, data)
                    except Exception:
                        msg = "{0} -> {1} -> {2} -> parseData()"
                        glob.ERRLOGGER.exception(msg.format(self.__class__.__name__, pair, self.status[pair]))
                    else:
                        self.save_status(pair, next_chunk)
                else:
                    msg = "{0} -> {1} finished. Next turn will start at {2}."
                    glob.SYSLOGGER.info(msg.format(self.__class__.__name__, pair, self.status[pair]))
                    raise Finished
                turns += 1

    def save_status(self, trade_pair=None, ts=0):
        if trade_pair:
            self.status[trade_pair] = ts
        with open(self.status_file + ".temp", "wb") as status_f:
            pickle.dump(self.status, status_f)
        replace(self.status_file + ".temp", self.status_file)

    async def on_task_start(self):
        if not self.tradepairs:
            await self.load_markets()
            self.tradepairs = self.exchange.symbols
        self._validated_tradpairs = await self.get_validated_tradepairs()

    async def on_grabber_quit(self):
        self.save_status()

    async def handle_exception(self, e, pair, current_chunk_id):
        if isinstance(e, (CcxtBaseError, ClientError, KeyError)):
            if self.retry_times < self.max_retry and not self.quit:
                delay = 5 << self.retry_times
                msg = "{0} will retry {1} -> {2} in {3} seconds."
                glob.SYSLOGGER.info(msg.format(self.__class__.__name__,
                                               pair,
                                               current_chunk_id,
                                               delay))
                await self.sleep(delay)
                raise RetryLatest
            else:
                msg = "The max retry of {0} exceed."
                glob.SYSLOGGER.info(msg.format(self.__class__.__name__))
                raise Abandon
        else:
            raise e
