#!/usr/bin/env python
# coding=utf-8
# author: dz_lee

import calendar
import datetime

import baostock as bs
from GetStockData.Get import Get_Stock_Data_From_BAOSTOCK as get_bs

# 因为是要从baostock 调用数据所以日期格式参照他们的要求
TODAY_BS = datetime.datetime.strftime(datetime.datetime.today(), "%Y-%m-%d")


class Calendar_tool():
    def __init__(self):
        self.day = TODAY_BS

    def is_trading_day(self):
        """
        判断当前日期是否为交易日
        :return:
        """
        bs.login()
        # 注意
        df_trading_day = get_bs(start_date=self.day, end_date=self.day).get_trade_date()
        bs.logout()

        result = df_trading_day['is_trading_day'][0]
        if result:
            # print("当天为交易日，开始获取当日交易数据......")
            return True
        else:
            # print("当天为非交易日，不用获取当日交易数据......")
            return False

    def is_saturday(self):
        # 假如今天是周六
        # 注意
        day = datetime.datetime.strptime(str(self.day), "%Y-%m-%d")
        is_saturday = day.weekday()
        # 如果今天是周六，则返回True
        if is_saturday == 5:
            return True
        else:
            return False

    def is_end_of_month(self):
        year = datetime.datetime.strptime(str(self.day), '%Y-%m-%d').year
        month = datetime.datetime.strptime(str(self.day), '%Y-%m-%d').month
        # 返回一个元组，第一个整数:代表本月起始星期数(0:星期一 ... 6:星期天) ,第二个整数:代表本月最后一天的日期数
        _, lastday = calendar.monthrange(year, month)
        end_of_month = datetime.datetime(year, month, lastday).date()

        # 如果今天是周末，返回True
        # 注意
        if str(self.day) == str(end_of_month):
            return True
        else:
            return False
