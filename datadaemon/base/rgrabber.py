# -*- coding: utf-8 -*-
from datadaemon.base import CCXTGrabber
from os import path
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from collections import defaultdict
from datadaemon.utilities.glob import CONFIG


class rCCXTGrabber(CCXTGrabber):
    def __init__(self):
        self.db_conn = None
        self.cols = ["timestamp_ms", "bar_time", "open", "high", "low", "close", "vol"]
        super(rCCXTGrabber, self).__init__()
        self.tradepair_last_update = defaultdict(str)
        self.status_file = path.join(self.dumploc, "{name}_status.db".format(name=self.__class__.__name__))
        self.dtype = {"timestamp_ms": "int64",
                 "open": "float64",
                 "high": "float64",
                 "low": "float64",
                 "close": "float64",
                 "vol": "float64"}

    @property
    def status(self):
        if (not self._status) and path.exists(self.status_file):
            conn = create_engine("sqlite:///" + self.status_file)
            df = pd.read_sql_table("save", conn, columns=["tradepair", "latest_ts"])
            self._status = defaultdict(int, **dict(zip(df["tradepair"],
                                                       df["latest_ts"])))
        return self._status

    async def save_status(self, trade_pair=None, ts=0, dump=False):
        if trade_pair:
            self.status[trade_pair] = ts
            self.tradepair_last_update[trade_pair] = datetime.now(tz=CONFIG.timezone).strftime(
                "%Y-%m-%d %H:%M:%S")
        if dump:
            conn = create_engine("sqlite:///" + self.status_file)

            df = pd.DataFrame.from_records(({"tradepair": i[0],
                                            "latest_ts": i[1],
                                             "latest_bartime": datetime.fromtimestamp(i[1] // 1000,
                                                                                      tz=CONFIG.timezone).strftime(
                                                 "%Y-%m-%d %H:%M:%S"),
                                             "last_update": self.tradepair_last_update[i[0]]} for i in self.status.items()),
                                           columns=["tradepair", "latest_ts", "latest_bartime", "last_update"],
                                           )
            df.to_sql("save", conn, index=False, if_exists="replace")

    def save(self, pair, data):
        df = self.fill(pd.DataFrame.from_records(data, columns=self.cols))
        archive_table = self.get_archive_table(pair)
        if self.db_conn:
            dst_conn = create_engine(self.db_conn)
        else:
            dst_conn = create_engine("sqlite:///" + self.local_buffer)
        df.to_sql(archive_table, dst_conn,
                  index=False, if_exists="append")

    def parseData(self, pair, data):
        if data:
            parsed_data = ({"timestamp_ms": i[0],
                            "bar_time": datetime.fromtimestamp(i[0] // 1000,
                                                               tz=CONFIG.timezone).strftime(
                                "%Y-%m-%d %H:%M:%S"),
                            "open": i[1],
                            "high": i[2],
                            "low": i[3],
                            "close": i[4],
                            "vol": i[5]} for i in data)
            self.save(pair, parsed_data)

    @property
    def local_buffer(self):
        return path.join(self.dumploc, "{0}.db".format(self.__class__.__name__))

    def fill(self, df):
        if (not df.empty)\
                and len(df) < (int(df["timestamp_ms"].iloc[-1]) - int(df["timestamp_ms"].iloc[0])) // 60000 + 1:
            result = pd.DataFrame()
            result["timestamp_ms"] = range(int(df["timestamp_ms"].iloc[0]), int(df["timestamp_ms"].iloc[-1]) + 1, 60000)
            result["bar_time"] = result["timestamp_ms"].apply(
                lambda row: datetime.fromtimestamp(row // 1000, tz=CONFIG.timezone).strftime("%Y-%m-%d %H:%M:%S"))
            result = result.merge(df.drop("bar_time", axis=1), on="timestamp_ms", how="outer")
            result["vol"].fillna(0, inplace=True)
            result["close"].fillna(method="ffill", inplace=True)
            result = result.T
            result.fillna(method="bfill", inplace=True)
            return result.T.astype(self.dtype)
        return df

    def get_archive_table(self, pair):
        base_currency, quote_currency = pair.split("/")
        archive_table = "{b}-{q}".format(b=base_currency,
                                         q=quote_currency)
        return archive_table
