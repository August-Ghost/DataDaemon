# coding: utf-8 -*-
from .shepherd import Shepherd
import signal
from .utilities import glob
from sys import exit, platform
import asyncio


class Mainframe:
    def __init__(self, **kargs):
        self.shepherd = Shepherd(**kargs)
        self.signal_mapping = {signal.SIGINT: signal.signal(signal.SIGINT, self.terminate),
                               signal.SIGTERM: signal.signal(signal.SIGTERM, self.terminate),}
        if platform == "win32":
            self.signal_mapping[signal.SIGBREAK] = signal.signal(signal.SIGBREAK, self.terminate)
        self.termination_in_progress = False

    def launch(self):
        glob.SYSLOGGER.info("DataDaemon on {0} start.".format(glob.NODE_NAME))
        glob.CURRENT_EVENTLOOP.set_exception_handler(self.loop_exception_handler)

        self.shepherd.shepherd()
        glob.CURRENT_EVENTLOOP.call_soon(self.wake_up)
        glob.CURRENT_EVENTLOOP.run_forever()

    def wake_up(self):
        glob.CURRENT_EVENTLOOP.call_later(0.1, self.wake_up)

    def terminate(self, signum, frame):
        if not self.termination_in_progress:
            async def killer():
                execution = asyncio.ensure_future(self.terminator(exclude_task=kill_task),
                                                  loop=glob.CURRENT_EVENTLOOP)
                while not execution.done():
                    await asyncio.sleep(0.3)
                else:
                    await glob.CURRENT_EVENTLOOP.shutdown_asyncgens()
                    msg = "DataDaemon on {0} shutdown due to signal {1} received."
                    glob.SYSLOGGER.info(msg.format(glob.NODE_NAME, signum))
                    exit(0)

            kill_task = asyncio.ensure_future(killer(), loop=glob.CURRENT_EVENTLOOP)



    def loop_exception_handler(self, loop, context):
        async def killer():
            execution = asyncio.ensure_future(self.terminator(exclude_task=kill_task),
                                            loop=loop)
            while not execution.done():
                await asyncio.sleep(0.3)
            else:
                if glob.ALERTOVER_NOTIFIER:
                    msg = "DataDaemon on {0} shutdown unexpectedly. Context: {1}"
                    await glob.ALERTOVER_NOTIFIER(
                        urgent=True,
                        content=msg.format(glob.NODE_NAME, context),
                        loop=loop
                    )
                await glob.CURRENT_EVENTLOOP.shutdown_asyncgens()
                msg = "DataDaemon on {0} shutdown due to an unhandled exception."
                glob.SYSLOGGER.info(msg.format(glob.NODE_NAME))
                exit(1)
        kill_task = asyncio.ensure_future(killer(), loop=loop)

    async def terminator(self, exclude_task=None):
        self.shepherd.shutdown()
        if exclude_task:
            if hasattr(exclude_task, "__iter__") and not isinstance(exclude_task, asyncio.Task):
                exclude = set(exclude_task)
            else:
                exclude = {exclude_task}
        else:
            exclude = {}
        while 1:
            all_done = True
            for task in asyncio.Task.all_tasks():
                if task != asyncio.Task.current_task() and task not in exclude:
                    all_done = all_done and task.done()
            if not all_done:
                await asyncio.sleep(0.3)
            else:
                break