#!/usr/bin/env python
# coding=utf-8
# author: dz_lee
import asyncio

import baostock as bs
import pandas as pd
from tqdm import tqdm
from SaveData.SaveToMongodb import MyMongodb as save


class MyBaostock():
    def __init__(self, end_date=None, start_date=None, one_day=None):
        # 起始日
        self.start_date = start_date
        # 截止日
        self.end_date = end_date
        # 指定日期
        self.one_day = one_day

    def get_trade_data(self):
        """
        获取股票交易日信息, 通过 start_date, end_date 参数设定起止日期数据, 返回每日是否是交易日
        :return:calendar_date:日期； is_trading_day:是否交易日(0:非交易日;1:交易日); DataFrame类型数据
        """
        try:
            data = bs.query_trade_dates(start_date=self.start_date, end_date=self.end_date).get_data()
            return data
        except Exception as e:
            print(f"交易日查询功能出错,错误原因：{e}")

    def get_stock_code(self):
        """
        获取指定交易日期证券信息
        :return:DataFrame类型数据,包含证券代码（A股、指数）、交易状态、证券名称
        """
        try:
            data = bs.query_all_stock(day=self.one_day).get_data()
        except Exception as e:
            print(f"证券代码查询功能出错,错误原因：{e}")

        return data

    def get_one_stock_infor(self, code):
        """
        获取单只证券基本资料
        :param code:证券代码
        :return:单只证券基本资料包含证券代码、证券名称、上市日期、退市日期、证券类型、上市状态; DataFrame 类型数据
        """
        try:
            data = bs.query_stock_basic(code).get_data()
            # print(f"正在获取{code}的信息......")
        except Exception as e:
            print(f"查询股票--{code}发生错误，错误原因：{e}")

        return data

    def get_all_stock_infor(self):
        """
        获取证券基本资料，可以通过参数设置获取对应证券代码、证券名称、上市日期、退市日期、证券类型和上市状态。
        本方法是获取所有证券代码（含股票、指数等），通过遍历每个证券基本资料中的证券类型、上市状态，选取未退市的股票代码。
        :return:符合要求的股票代码
        """
        # 除北交所外，其他证券代码
        stock_code_list = []
        # 北交所证券代码,目前暂未使用
        bj_code = []

        # 第1步：异步方式读取 trade_status 数据, 获取所有证券代码信息
        loop = asyncio.get_event_loop()
        read_data = save(database="stock_information", collection="stock_code") \
            .find(query=None, display_item={"code": 1}, onlyone=False)
        stock_code_data = loop.run_until_complete(read_data)

        for item in stock_code_data:
            code = item["code"]
            if code.startswith("bj."):
                bj_code.append(code)
            else:
                stock_code_list.append(code)

        # 第2步：通过遍历每个证券基本资料中的证券类型、上市状态，选取除北交所以外的所有证券代码
        for code in tqdm(stock_code_list, desc="正在获取证券基本资料"):
            new_data = self.get_one_stock_infor(code)

            try:
                loop = asyncio.get_event_loop()
                stock_basic_information = save(database="stock_information", collection="stock_basic_information") \
                    .insert(new_data, _index="code", onlyone=False)
                loop.run_until_complete(stock_basic_information)
            except Exception as e:
                print(f"证券信息({code})获取失败，错误原因：{e}")

    def get_k_line_data(self, field, frequency):
        """
        获取指定日期股票数据
        :param field:股票指标，日线、周线、月线不同指标内容，详见BS历史行情指标参数章节
        :param frequency:数据类型，日线、周线、月线以及分钟线。目前能力不行，暂时不获取分钟线（下载历史数据太费事，我自己数学功底不够）
        :return:股票数据
        """
        # 第1步：从数据库中获取证券代码
        loop = asyncio.get_event_loop()
        tasks = save(database="stock_information", collection="stock_basic_information") \
            .find(query={"status": "1"}, display_item={"code": 1})
        stock_codes = loop.run_until_complete(tasks)

        # 第2步：获取指定日期证券历史数据并保存到数据库内
        for item in tqdm(stock_codes, desc="历史股价下载进度"):
            code = item["code"]
            # bs.query_history_k_data_plus 方法中能获取的最早的历史数据是1990-12-19 ，所以start_date 设置默认值
            price_data = bs.query_history_k_data_plus(code, fields=field, frequency=frequency,
                                                      end_date=self.end_date, start_date="1990-12-19",
                                                      adjustflag="3").get_data()
            loop = asyncio.get_event_loop()
            database_name = f"stock_price_{frequency}_line"
            collection_name = code
            # 这里要注意，保存股价数据 _index 是选取 date 不是 code
            stock_code = save(database=database_name, collection=collection_name) \
                .insert(price_data, _index="date", onlyone=False)
            loop.run_until_complete(stock_code)
        return None

    def down_k_line(self):
        for frequency in ['d', 'w', 'm']:
            if frequency in ["w", "m"]:
                field = "date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg"
            else:
                field = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn," \
                        "tradestatus,pctChg,peTTM,psTTM,pcfNcfTTM,pbMRQ,isST"

            self.get_k_line_data(field, frequency)
        return None