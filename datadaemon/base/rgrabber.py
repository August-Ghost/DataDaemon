# -*- coding: utf-8 -*-
from datadaemon.base import CCXTGrabber
from os import path
import pandas as pd
from datetime import datetime
import sqlite3
from sqlalchemy import create_engine
from collections import defaultdict
import pickle
from datadaemon.utilities import glob


class rCCXTGrabber(CCXTGrabber):
    def __init__(self):
        self.db_conn = None
        self.erase_buffer = defaultdict(lambda: True)
        self.cols = ["timestamp_ms", "bar_time", "open", "close", "high", "low", "vol"]

        super(rCCXTGrabber, self).__init__()

    @property
    def status(self):
        if (not self._status):
            if path.exists(self.status_file) and not path.exists(self.status_file + ".temp"):
                try:
                    with open(self.status_file, "rb") as status_f:
                        self._status = pickle.load(status_f)
                except Exception as e:
                    self.recover()
            elif path.exists(self.status_file + ".temp"):
                self.recover()
        return self._status

    def dump_to_buffer(self, pair, data):
        df = pd.DataFrame.from_records(data, columns=self.cols)
        conn = create_engine("sqlite:///" + self.local_buffer)
        df.to_sql(self.get_archive_table(pair),
                  conn, index=False,
                  if_exists="append" if not self.erase_buffer[pair] else "replace")
        self.erase_buffer[pair] = False

    def on_tradepair_done(self, pair):
        # Deliver data to remote database.
        if path.exists(self.local_buffer) and self.db_conn:
            archive_table = self.get_archive_table(pair)
            src_conn = create_engine("sqlite:///" + self.local_buffer)
            df = pd.pandas.read_sql_table(archive_table, src_conn)
            dst_conn = create_engine(self.db_conn)
            df.to_sql(archive_table, dst_conn,
                      index=False, if_exists="append")
            self.erase_buffer[pair] = True

    def get_archive_table(self, pair):
        base_currency, quote_currency = pair.split("/")
        archive_table = "{b}-{q}".format(b=base_currency,
                                         q=quote_currency)
        return archive_table

    def on_task_done(self):
        if path.exists(self.local_buffer) and self.db_conn:
            conn = create_engine("sqlite:///" + self.local_buffer)
            pd.io.sql.execute('VACUUM', conn)

    @property
    def local_buffer(self):
        return path.join(self.dumploc, "{0}.db".format(self.__class__.__name__))

    def recover(self):
        # Try to recover data status from local buffer
        if path.exists(self.local_buffer):
            for pair in self.buffered_tradepairs:
                sql = """
                SELECT timestamp_ms + 1 AS latest
                FROM "{0}"
                WHERE  timestamp_ms == (
                    SELECT max("timestamp_ms")
                    FROM "{0}")
                """.format(self.get_archive_table(pair))

                conn = create_engine("sqlite:///" + self.local_buffer)
                df = pd.read_sql_query(sql, conn)
                if not df["latest"].empty:
                    self._status[pair] = df["latest"].iloc[0]
                else:
                    self._status[pair] = 0
            msg = "{0} recovered tradepair status from local buffer. "
            glob.SYSLOGGER.info(msg.format(self.__class__.__name__))
        else:
            msg = "Local buffer absent. Unable to recover tradepair status for {0}."
            raise RuntimeError(msg.format(self.__class__.__name__))

    @property
    def buffered_tradepairs(self):
        if path.exists(self.local_buffer):
            sql = """
                    SELECT "tbl_name"
                    FROM "sqlite_master"
                    """
            conn = create_engine("sqlite:///" + self.local_buffer)
            df = pd.read_sql_query(sql, conn)
            if not df["tbl_name"].empty:
                return [pair.replace("-", "/") for pair in df["tbl_name"]]
        return []
