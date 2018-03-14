# DataDaemon
A cryptocurrency market data collecting tool.


## Feature

- Asynchronous program based on Python 3
- Hotfix support. When enabled, you can deploy new grab task without interrupting running task.
- Built-in message pushing support. When enabled, a brief info will be delivered to your mobile device if the server crashed.
- Easy to use. You can set up a grab task in a minute.


## Install

```
pip install datadaemon
```

## Example

Grab task example
```python
# -*- coding: utf-8 -*-
from datadaemon.base import rCCXTGrabber
import ccxt.async as acx


class BinanceKlineGrabber(rCCXTGrabber):
    def config(self):
        return {
            "run_period": 7200,
            "exchange": acx.binance(),
            "tradepairs": None,
            "ratelimit": 240,
            "query":{
                "timeframe": "1m",
                "since":None,
                "params":{}
            },
            "dumploc": r"G:\Project\Python\crypto_analytics\data\binance",
            "cols": ["timestamp_ms", "bar_time", "open", "high", "low", "close", "vol"],
            "db_conn": r"sqlite:///G:\Project\Python\crypto_analytics\data\binance\binance_archive.db"
        }

    async def fetch_data_from_exchange(self, pair, since):
        partial_data = await self.exchange.fetch_ohlcv(pair, **{**self.query, "since": since})
        if partial_data:
            return partial_data[-1][0] + 1, partial_data
        else:
            return None, None


EXPORT = BinanceKlineGrabber
```

Config example:
```python
from datetime import timezone, timedelta
from os import  getcwd
from os.path import join

CONFIG.workdir = r"G:\Project\Python\crypto_analytics\datadaemon\grabbers"
CONFIG.verbose = True
CONFIG.enable_debug = False
CONFIG.enable_systemlog = True
CONFIG.enable_accesslog = False
CONFIG.enable_errorlog = True
CONFIG.enable_autoreload = False
CONFIG.enable_alertover = True
CONFIG.dbglog = join(getcwd(), "DataDaemon_debug.log")
CONFIG.accesslog = join(getcwd(), "DataDaemon_access.log")
CONFIG.errlog = join(getcwd(), "DataDaemon_error.log")
CONFIG.syslog = join(getcwd(), "DataDaemon_system.log")
CONFIG.autoreload_period = 10
CONFIG.alertover_source_id = "EXAMPLE"
CONFIG.alertover_group_id = "EXAMPLE"
CONFIG.timezone = timezone(timedelta(hours=8))
```