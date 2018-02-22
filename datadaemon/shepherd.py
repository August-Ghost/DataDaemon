# -*- coding:utf-8 -*-
from asyncio import ensure_future, sleep
from os import stat
from os.path import split, splitext, basename, abspath
from glob import iglob
from sys import path as syspath
from .utilities import glob


class Shepherd:
    def __init__(self, workdir="./grabbers", auto_reload=True,
                 reload_period=10):
        self.grabber_path = abspath(workdir)
        self.auto_reload = auto_reload if auto_reload else False
        self.reload_period = reload_period if reload_period > 0 else 10
        self.grabber_path_mtime = 0
        # path: (mtime, grabber)
        self.watch_list = {}
        self.task = None

    def get_module(self, excluded="__init__.py"):
        excluded = set("{0}".format(splitext(basename(ex))[0])
                                    for ex in ({excluded} if isinstance(excluded, str) else set(excluded)))
        return set("{0}".format(splitext(basename(path))[0])
                   for path in iglob("{0}/*.py".format(self.grabber_path))) - excluded

    def shepherd(self):
        syspath.append(self.grabber_path)
        self.task = ensure_future(self._shepherd())

    async def _shepherd(self):
        while 1:
            start = glob.CURRENT_EVENTLOOP.time()
            mtime = stat(self.grabber_path).st_mtime
            if mtime > self.grabber_path_mtime:
                prototype = {}
                for grabber_module_name in self.get_module():
                    try:
                        grabber_prototype = __import__(grabber_module_name,
                                               globals(),
                                               locals(),
                                               ["EXPORT"])
                    except Exception as e:
                        glob.SYSLOGGER.exception("importing {0} failed.".format(grabber_module_name))
                    else:
                        if not hasattr(grabber_prototype, "EXPORT"):
                            continue
                        else:
                            #path: prototype
                            prototype[grabber_prototype.__file__] = grabber_prototype.EXPORT

                # Figure out the newly-addeds, the deleteds, the modifieds
                modified = [p for p in self.watch_list if stat(p).st_mtime > self.watch_list[p][0]]
                self.retire_watch_list_items(modified)
                deleted = set(self.watch_list) - set(prototype.keys())
                # Kill deleted grabbers
                self.retire_watch_list_items(deleted)

                added = {}
                for p in set(prototype.keys()) - set(self.watch_list):
                    try:
                        added[p] = (stat(p).st_mtime, prototype[p]())
                    except Exception as e:
                        glob.ERRLOGGER.exception("Failed to instantiate {0}.".format(prototype[p].__name__))
                    else:
                        glob.SYSLOGGER.info("{0} instantiated.".format(prototype[p].__name__))
                for item in added.values():
                    item[1].start()
                self.watch_list.update(added)

            self.grabber_path_mtime = mtime
            if not self.auto_reload:
                break
            delay = glob.CURRENT_EVENTLOOP.time() - start
            await sleep(delay if delay > 0 else 0)

    def shutdown(self):
        for k, v in self.watch_list.items():
            v[1].stop()
        self.task.cancel()

    def retire_watch_list_items(self, src):
        for p in src:
            self.watch_list[p][1].stop()
            glob.SYSLOGGER.info("{0} retired".format(self.watch_list[p][1].__class__.__name__))
            del self.watch_list[p]