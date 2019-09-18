# -*- coding: utf-8 -*-
# @Time    : 2019/9/18 20:59
# @Author  : CRJ
# @File    : BloomFilter.py
# @Software: PyCharm
# @Python3.6
try:
    import bitarray
except ImportError:
    raise ImportError('Requires bitarray')

try:
    import mmh3
except ImportError:
    raise ImportError('Requires mmh3')


class BloomFilter(object):

    def __init__(self, capacity, error_rate=0.001):
        """

        :param capacity: 所需存放数据的数量
        :param error_rate:  可接受的误报率，默认0.001

        通过这两个参数来确定需要多少个哈希函数
        """

        if not capacity > 0:
            raise ValueError("Capacity must be > 0")
        if not (0 < error_rate < 1):
            raise ValueError("Error_Rate must be between 0 and 1.")

        self.capacity = capacity
        self.error_rate = error_rate

        bit_num, hash_num = self._adjust_param(capacity, error_rate)
        self.bit_array = bitarray(bit_num)
        self.bit_array.setall(0)
        self.hash_num = hash_num

        self.__data_count = 0

    def _adjust_param(self, capacity, error_rate):
        """


        :param capacity:
        :param error_rate:
        :return:

        通过数据量和期望的误报率 计算出 位数组大小 和 哈希函数的数量
        """
        pass

    def add(self, key):
        pass

    def exists(self, key):
        pass

    def copy(self):
        """

        复制一份布隆过滤器的实例
        :return:
        """
        pass

    def __len__(self):
        """"
        返回现有数据容量
        """
        return self.__data_count

    def __contains__(self, key):
        """

        :param key:
        :return:

        用于实现 in 判断
        """
        return self.exists(key)
