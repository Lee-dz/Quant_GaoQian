#!/usr/bin/env python
# coding=utf-8
# author: dz_lee
import time


class Timer():
    def __init__(self, func):
        self._func = func

    def __get__(self, ins, cls):
        """
        :param ins:代表实例
        :param cls:代表类本身
        :return:
        """
        start_time = time.perf_counter()
        self._func(ins)
        end_time = time.perf_counter()
        print(f'函数:{self._func.__name__} 运行完成，共耗时{end_time - start_time}秒')
