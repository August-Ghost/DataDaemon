# -*- coding: utf-8 -*-
from .utilities import glob, log
import logging
import alertover.async as aao
from os import getcwd
from os.path import join, basename, dirname, abspath, splitext
from .mainframe import Mainframe

def create(name, template_file, dst):
    from string import Template
    with open(template_file, "r") as t:
        template = Template(t.read())
        with open(join(dst, "{0}.py".format(name)), "w") as d:
            d.write(template.safe_substitute(grabber_name=name.capitalize()))


def setVerbose(enable=False):
    if enable:
        hdlr = log.getHandler(logging.StreamHandler)
        glob.ROOTLOGGER.addHandler(hdlr)


def setDebug(enable=False, dbglog=""):
    if enable:
        if dbglog:
            logging.getLogger('asyncio').addHandler(log.getFileHandler(dbglog))
        logging.getLogger('asyncio').addHandler(log.getHandler(logging.StreamHandler))
        glob.CURRENT_EVENTLOOP.set_debug()
    else:
        logging.getLogger('asyncio').setLevel(logging.WARNING)


def setAccessLog(enable=False, acclog=""):
    if enable:
        if acclog:
            hdlr = log.getFileHandler(acclog)
        else:
            hdlr = log.getFileHandler(join(getcwd(), "DataDaemon_access.log"))
        glob.ACCESSLOGGER.addHandler(hdlr)


def setErrorLog(enable=False, errlog=""):
    if enable:
        if errlog:
            hdlr = log.getFileHandler(errlog)
        else:
            hdlr = log.getFileHandler(join(getcwd(), "DataDaemon_error.log"))
        glob.ERRLOGGER.addHandler(hdlr)


def setSystemLog(enable=False, syslog=""):
    if enable:
        if syslog:
            hdlr = log.getFileHandler(syslog)
        else:
            hdlr = log.getFileHandler(join(getcwd(), "DataDaemon_sys.log"))
        glob.SYSLOGGER.addHandler(hdlr)


def setAlertOver(enable, source, receiver):
    if enable and (source and receiver):
        hdlr = log.getHandler(aao.AlertOverHandler, lvl=logging.ERROR,
                              source=source, receiver=receiver, loop=glob.CURRENT_EVENTLOOP)

        hdlr.set_default_msg_attr(title="From: {0}".format(glob.NODE_NAME),
                                  sound="system")
        glob.ERRLOGGER.addHandler(hdlr)

        glob.ALERTOVER_NOTIFIER = log.AlertOverSender(source=source,
                                                       receiver=receiver)

def main():
    import sys
    from .utilities.glob import CONFIG
    sys_args = sys.argv[1:]
    if not sys_args:
        raise AttributeError("You must specify a config file for DataDaemon.")
    else:
        sys_args.append(abspath("."))
        insert_index = len(sys.path)
        sys.path.append(dirname(sys_args[0]))
        try:
            with open(sys_args[0], "r") as config:
                exec(config.read())
        except Exception:
            raise RuntimeError("Invalid config: {0}".format(sys_args[0]))
        else:
            sys.path.pop(insert_index)
            setVerbose(glob.CONFIG.verbose)
            setDebug(glob.CONFIG.enable_debug, glob.CONFIG.dbglog)
            setAccessLog(glob.CONFIG.enable_accesslog, glob.CONFIG.accesslog)
            setSystemLog(glob.CONFIG.enable_systemlog, glob.CONFIG.syslog)
            setErrorLog(glob.CONFIG.enable_errorlog, glob.CONFIG.errlog)
            setAlertOver(glob.CONFIG.enable_alertover,
                         glob.CONFIG.alertover_source_id,
                         glob.CONFIG.alertover_group_id)
            main = Mainframe(workdir=glob.CONFIG.workdir,
                             auto_reload=True if glob.CONFIG.enable_autoreload else False,
                             reload_period=glob.CONFIG.autoreload_period if glob.CONFIG.autoreload_period > 0 else 10)
            main.launch()