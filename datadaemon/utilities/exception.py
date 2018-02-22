# -*- coding: utf-8 -*-

class DDException(Exception):
    pass

class Finished(BaseException):
    pass

class RetryLatest(BaseException):
    pass

class Break(BaseException):
    pass

class Terminate(BaseException):
    pass

class Abandon(BaseException):
    pass

