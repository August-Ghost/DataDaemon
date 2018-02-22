# -*- coding: utf-8 -*-
from asyncio import get_event_loop
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import logging
from platform import system
import socket
from os.path import join
from os import getcwd
import alertover.async as aao
from sys import modules


class Config:
    def __init__(self):
        self.workdir = getcwd()
        self.verbose = False
        self.enable_debug = False
        self.enable_systemlog = False
        self.enable_accesslog = False
        self.enable_errorlog = False
        self.enable_autoreload = False
        self.enable_alertover = False
        self.dbglog = join(getcwd(), "DataDaemon_debug.log")
        self.accesslog = join(getcwd(), "DataDaemon_access.log")
        self.errlog = join(getcwd(), "DataDaemon_error.log")
        self.syslog = join(getcwd(), "DataDaemon_system.log")
        self.autoreload_period = 10
        self.alertover_source_id = None
        self.alertover_group_id = None
        self.grabber_glob_config = {}

class Glob:
    def __init__(self):
        self.CONFIG = Config()
        self.NODE_NAME = socket.getfqdn()
        self.CURRENT_EVENTLOOP = get_event_loop()
        self.ROOTLOGGER = logging.getLogger("DataDaemon")
        self.ROOTLOGGER.setLevel(logging.INFO)
        self.SYSLOGGER = self.ROOTLOGGER.getChild("SystemLog")
        self.ERRLOGGER = self.ROOTLOGGER.getChild("ErrorLog")
        self.ACCESSLOGGER = self.ROOTLOGGER.getChild("AccessLog")
        self.ALERTOVER_NOTIFIER = None
        self.THREADPOOLEXEC = ThreadPoolExecutor()
        if system() == "Windows":
            self.PROCESSPOOLEXEC = self.THREADPOOLEXEC
        else:
            self.PROCESSPOOLEXEC = ProcessPoolExecutor()

    def __setattr__(self, key, value):
        object.__setattr__(self, key, value)


modules[__name__] = Glob()
