# -*- coding: utf-8 -*-
import logging
import traceback
from os.path import join
import alertover.async as aao



#logging.getLogger().addHandler(logging.NullHandler())

DEFAULT_LOGGER_FORMAT  = "%(asctime)s | %(name)s | %(levelno)s | %(message)s"


def getHandler(hdlr_cls, lvl=logging.DEBUG, format=None, *args, **kargs):
    formatter = logging.Formatter(DEFAULT_LOGGER_FORMAT if not format \
                                      else format)
    hdlr = hdlr_cls(*args, **kargs)
    hdlr.setFormatter(formatter)
    if lvl:
        hdlr.setLevel(lvl)
    return hdlr


def getFileHandler(logfile, lvl=logging.DEBUG, format=None):
    return getHandler(logging.FileHandler, lvl, format, logfile)


def AlertOverSender(source, receiver):
    async def sender(title="",
                     urgent=False,
                     sound="default",
                     content="",
                     url="",
                     loop=None):
        await aao.send(source, receiver,
                       title=title,
                       urgent=urgent,
                       sound=sound,
                       content=content,
                       url=url,
                       loop=loop)
    return sender


def dump_exception(e, dump_loc, filename=None):
    ei = (type(e), e, e.__traceback__)
    tb = ei[2]
    if filename:
        dump_file = join(dump_loc, filename)
    else:
        from datetime import datetime
        dump_file = join(dump_loc, "exception-{0}.log".format(int(datetime.now().timestamp())))

    with open(dump_file, "w") as sio:
        traceback.print_exception(ei[0], ei[1], tb, None, sio)
    return dump_file, exec_brief_info(e)


def exec_brief_info(e):
    ei = (type(e), e, e.__traceback__)
    tb = ei[2]
    return "".join(traceback.format_exception(ei[0], ei[1], tb)[-3:])