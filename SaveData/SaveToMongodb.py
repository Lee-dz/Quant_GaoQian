#!/usr/bin/env python
# coding=utf-8
# author: dz_lee

# 导入pymongo模块
import pymongo
from pymongo import errors
import motor.motor_asyncio

class MyMongodb():
    def __init__(self, database, collection):
        """
        利用初始化的方法输入要操作的数据库（database）,集合（collection）
        :param database:
        :param collection:
        """
        try:
            # 连接到虚拟机上的mongo数据库
            self.connet = motor.motor_asyncio.AsyncIOMotorClient("localhost", 27017)
            # 选择需要操作的数据库名称
            self.database = self.connet[database]
            # 选择需要操作的集合名称（如果集合名不存在会自动创建）
            self.collection = self.database[collection]
        except Exception as e:
            print(f"无法连接数据库,原因：{e}")

    def converted_data(self, data):
        '''
        数据转换
        :param data:初始数据
        :return:目标数据
        '''
        new_data = data.to_dict(orient='records')
        return new_data

    # 输入需要添加的数据（data）
    async def insert(self, data, _index=None, onlyone=False):
        """
        将数据插入指定的数据库
        :param data:新数据
        :return:
        """
        try:
            if onlyone:
                # data 数据类型为dict
                self.collection.insert_one(data)
            else:
                # 转换数据，向集合中增加多条数据; 多组数据时候传入的是个由字典组成的列表
                data = self.converted_data(data)
                for item in data:
                    condition = {_index:item[_index]}
                    await self.collection.update_one(condition, {"$set":item}, upsert=True)
            # print("数据插入完成......")
        except Exception as e:
            print(f"数据插入错误，错误原因：{e}")

    async def update(self, data, new_data, onlyone=False):
        '''
        更新数据
        :param data:BS 上获取的数据, dataframe 类型.指定需要修改的数据
        :param new_data:修改后的数据
        :param onlyone:控制修改单条还是多条
        :return:
        '''
        try:
            if onlyone:
                # data/new_data 数据类型为dict
                await self.collection.update_one(data, {"$set": new_data}, upsert=True)
            else:
                # 转换数据
                data = self.converted_data(data)
                # 修改多条数据，使用'$set'表示指定修改数据否则会使数据库中所有数据被新数据覆盖
                await self.collection.update_many(data, {"$set": new_data}, upsert=True)

        except Exception as e:
            print(f"数据更新出错,原因：{e}")

    async def delete(self, data, onlyone=True):
        """
        删除数据，data需要删除的数据，使用onlyone控制删除的条数
        :param data:指定删除的对象
        :param onlyone:单独删除或者多条删除
        :return:
        """
        try:
            if onlyone:
                # data 数据类型为dict
                await self.collection.delete_one(data)
            else:
                await self.collection.delete_many(data)
            print("删除完成......")
        except Exception as e:
            print(f"数据删除出错，错误原因：{e}")

    async def find(self, query=None, display_item=None, onlyone=False):
        """
        输入查询的条件（query，默认为None指查询全部数据），使用onlyone控制查询的数据是单条还是多条
        :param query:使用查询操作符指定查询条件
        :param display_item:使用投影操作符指定返回的键。查询时返回你所需要的字段结果，多余的不显示
        :param onlyone:单条/多条查询
        :return:
        """
        # 默认onlyone为True查询符合条件的第一条数据
        if onlyone:
            # 将查询的结构用result变量来接收
            document = await self.collection.find_one(query)
            return document
        else:
            document = []
            # onlyone为False查询所有符合条件的条数据
            async for item in self.collection.find(query, projection=display_item):  # 查询所有文档
                document.append(item)
            return document


