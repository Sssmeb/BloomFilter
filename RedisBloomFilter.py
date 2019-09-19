# -*- coding: utf-8 -*-
# @Time    : 2019/9/19 22:48
# @Author  : CRJ
# @File    : RedisBloomFilter.py
# @Software: PyCharm
# @Python3.6
import redis
from BloomFilter import BloomFilter
import mmh3


class RedisFilter(BloomFilter):

    def __init__(self, host='localhost', port=6379, db=0, key='bloomfilter'):
        """

        :param host:
        :param port:
        :param db:
        :param key:
        """
        self.server = redis.Redis(host=host, port=port, db=db)
        self.key = key

    def start(self, data_size, error_rate=0.001):

        if not data_size > 0:
            raise ValueError("Capacity must be > 0")
        if not (0 < error_rate < 1):
            raise ValueError("Error_Rate must be between 0 and 1.")

        self.data_size = data_size
        self.error_rate = error_rate

        bit_num, hash_num = super()._adjust_param(data_size, error_rate)
        self._bit_num = bit_num if bit_num < (1 << 31) else (1 << 31)
        self._hash_num = hash_num

        # 将哈希种子固定为 1 - hash_num （预留持久化过滤的可能）
        self._hash_seed = [i for i in range(1, hash_num+1)]

        # 已存数据量
        self._data_count = 0

    def add(self, key):
        """
        :param key: 要添加的数据
        :return:

        >>> bf = BloomFilter(data_size=100000, error_rate=0.001)
        >>> bf.add("test")
        True

        """
        if self._is_half_fill():
            raise IndexError("The capacity is insufficient")

        for time in range(self._hash_num):
            key_hashed_idx = mmh3.hash(key, self._hash_seed[time]) % self._bit_num
            self.server.setbit(self.key, key_hashed_idx, 1)

        self._data_count += 1
        return True

    def is_exists(self, key):
        """
        :param key:
        :return:

        判断该值是否存在
        有任意一位为0 则肯定不存在
        """
        for time in range(self._hash_num):
            key_hashed_idx = mmh3.hash(key, self._hash_seed[time]) % self._bit_num
            if not int(self.server.getbit(self.key, key_hashed_idx)):    # 类型？
                return False
        return True

    def copy(self):
        """
        防止调用父类copy方法
        :return:
        """
        pass