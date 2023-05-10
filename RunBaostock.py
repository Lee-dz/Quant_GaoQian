#!/usr/bin/env python
# coding=utf-8
# author: dz_lee

import datetime

import pandas as pd
import baostock as bs
from SaveData.AutoUpdate import AutoSave
from GetStockData.GetDataFromBaostock import MyBaostock
from Tools.TimerTool import Timer


# 列名与数据对其显示
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)

if __name__ == "__main__":
    NOW_DAY = datetime.datetime.today().strftime("%Y-%m-%d")
    ONE_DAY = "2023-05-06"
    DatetimeOption = ONE_DAY

    # AutoSave 中的函数 auto_get_trading_day 被类装饰器 Timer 装饰，实际上将原本的类方法变为了类属性，所以调用时候不加()
    # start_date 初始值默认为"1990-12-19"，因为在BAOSTOCK里面获取K线历史数据最早就是这个日期，其他时间部分数据能获取但是如果拿不到股价历史数据就鸡肋啦
    # 更新交易日期数据
    AutoSave(start_date=DatetimeOption, end_date=DatetimeOption).automatic_download_trading_day
    # 更新证券代码数据
    AutoSave(start_date=DatetimeOption, end_date=DatetimeOption).automatic_download_stock_code
    # 更新证券基本信息(建议这一步修改成为周末调用来搜集，因为比较占用时间)
    AutoSave(start_date=DatetimeOption, end_date=DatetimeOption).automatic_download_stock_basic_information

    # # # 更新历史股价数据
    # # AutoSave(start_date="2022-08-01", end_date="2023-05-02").automatic_download_history_price_data
    # AutoSave(start_date="2023-05-03", end_date="2023-05-04").automatic_download_history_price_data




