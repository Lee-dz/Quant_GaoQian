#!/usr/bin/env python
# coding=utf-8
# author: dz_lee

import asyncio

import baostock as bs

from GetStockData.GetDataFromBaostock import MyBaostock as get
from SaveData.SaveToMongodb import MyMongodb as save
from Tools.TimerTool import Timer


class AutoSave():
    def __init__(self, start_date=None, end_date=None, one_day=None):
        """
        :param start_date:起始日
        :param end_date:终止日
        :param one_day指定某一日
        """
        self.start_date = start_date
        self.end_date = end_date
        self.one_day = one_day

    @Timer
    def automatic_download_trading_day(self):
        """
        第一步:更新股票交易日数据
        :return:
        """
        bs.login()
        print("第一步:更新股票交易日数据")

        trading_day_dt = get(start_date=self.start_date, end_date=self.end_date).get_trade_data()

        loop = asyncio.get_event_loop()
        # 这里要注意，_index 一定要看BaoStock 文档中数据返回的规定
        trade_date = save(database="stock_information", collection="trading_days") \
            .insert(trading_day_dt, _index="calendar_date", onlyone=False)
        loop.run_until_complete(trade_date)
        print("交易日期数据更新完毕......")

        bs.logout()

    @Timer
    def automatic_download_stock_code(self):
        """
        第二步：更新证券代码数据
        :return:
        """
        bs.login()
        print("第二步：更新证券代码数据")

        stock_code_dt = get(start_date=self.start_date, end_date=self.end_date, one_day=self.one_day).get_stock_code()
        loop = asyncio.get_event_loop()
        stock_code = save(database="stock_information", collection="stock_code") \
            .insert(stock_code_dt, _index="code", onlyone=False)
        loop.run_until_complete(stock_code)
        print("证券代码数据更新完毕......")

        bs.logout()

    @Timer
    def automatic_download_stock_basic_information(self):
        bs.login()

        print("第三步：更新证券基本信息")
        get(start_date=self.start_date, end_date=self.end_date, one_day=self.one_day).get_all_stock_infor()
        print("证券信息更新完毕......")

        bs.logout()

    @Timer
    def automatic_download_history_price_data(self):
        bs.login()

        print("第四步：更新证券历史股价")
        get(start_date=self.start_date, end_date=self.end_date, one_day=self.one_day).down_k_line()
        print("历史股价数据更新完毕......")

        bs.logout()
