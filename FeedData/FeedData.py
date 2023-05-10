#!/usr/bin/env python
# coding=utf-8
# author: dz_lee

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import backtrader as bt
import pandas as pd
import pymongo
from datetime import datetime
from backtrader import date2num


class MyMongodbData(bt.feed.DataBase):
    params = (
        ("amount", -1),
        ("turn", -1),
    )

    # 新增指标：AMOUNT-成交量，TURN-换手率
    lines = (
        "amount",
        "turn",
    )

    def __init__(self, db_name, coll_name, *args, **kwargs):
        #  继承父类性质
        super(MyMongodbData, self).__init__()
        # 断言.保证FROMDATE/TODATE两个参数不是空的
        assert (self.p.fromdate is not None)
        assert (self.p.todate is not None)

        # 数据库名字
        self.db_name = db_name
        # 集合名字.集合名称（我的数据库集合名字就是股票代码名字）
        self.coll_name = coll_name
        # 初始数据结果为空
        self.data = None
        # 定义个迭代对象
        self.iter = None

    def start(self):
        """
        从MONGODB中加载股价数据
        :param db_name:数据库名字
        :param coll_name:集合名字==股票名称，理由同上
        :param fromdate:起始日，注意这里必须是STRING格式，因为我从BAOSTOCK获取的数据，获取的数据都是STRING格式。所以必须数据处理
        :param todate:终止日，参考上一条
        :return:
        """
        # 如果数据为空的情况下连接MONGODB
        if self.data is None:
            client = pymongo.MongoClient("localhost", 27017)
            db = client[self.db_name]
            coll = db[self.coll_name]
            # 数据类型转变的原因详见函数注释
            fromdate = self.p.fromdate.strftime("%Y-%m-%d")
            todate = self.p.todate.strftime("%Y-%m-%d")

            self.data = coll.find({"date": {"$gte": fromdate, "$lte": todate}})

        # 将取得的数据转化为迭代器,下文中直接调用NEXT方法，所以要用迭代器
        self.iter = iter(self.data)

    def _load(self):
        if self.iter is None:
            return False

        try:
            one_row = next(self.iter)
        except StopIteration:
            return False

        # 将datetime格式的数据转变为UTC float days
        self.lines.datetime[0] = self.date2num(pd.to_datetime(one_row["date"]).date())
        # 以下其他的数据都要转变为FLOAT格式
        self.lines.open[0] = float(one_row["open"])
        self.lines.close[0] = float(one_row["close"])
        self.lines.high[0] = float(one_row["high"])
        self.lines.low[0] = float(one_row["low"])
        self.lines.volume[0] = float(one_row["volume"])
        self.lines.amount[0] = float(one_row["amount"])
        self.lines.openinterest[0] = -1

        # 调试的时候发现BAOSTOCK系统内很多时候换手率的值是个‘’
        if one_row["turn"] == '':
            one_row["turn"] = 0
        else:
            self.lines.turn[0] = float(one_row["turn"])

        return True


