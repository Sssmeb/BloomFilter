# -*- coding: utf-8 -*-
# @Time    : 2019/9/19 22:48
# @Author  : CRJ
# @File    : RedisBloomFilter.py
# @Software: PyCharm
# @Python3.6
import redis
from BloomFilter import BloomFilter
import mmh3
import math


class RedisFilter(BloomFilter):

    def __init__(self, host='localhost', port=6379, db=0, redis_key='bloomfilter_'):
        """

        :param host:
        :param port:
        :param db:
        :param key:
        """
        self.server = redis.Redis(host=host, port=port, db=db)
        self.redis_key = redis_key

        # 已存数据量
        self._data_count = 0

    def start(self, data_size, error_rate=0.001):
        """

        :param data_size: 所需存放数据的数量
        :param error_rate:  可接受的误报率，默认0.001
        :return:

        启动函数，通过数据量、误报率 确定位数组长度、哈希函数个数、哈希种子等
        """
        if not data_size > 0:
            raise ValueError("Capacity must be > 0")
        if not (0 < error_rate < 1):
            raise ValueError("Error_Rate must be between 0 and 1.")

        self.data_size = data_size
        self.error_rate = error_rate

        bit_num, hash_num = super()._adjust_param(data_size, error_rate)
        self._bit_num = bit_num if bit_num < (1 << 32) else (1 << 32)
        self._hash_num = hash_num
        # redis字符串最长为512M
        self._block_num = 1 if bit_num < (1 << 32) else math.ceil(math.ceil(bit_num/8/1024/1024)/512)
        # 将哈希种子固定为 1 - hash_num （预留持久化过滤的可能）
        self._hash_seed = [i for i in range(1, hash_num+1)]

        return True

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

        keyname = self.redis_key + str(sum(map(ord, key)) % self._block_num)

        for time in range(self._hash_num):
            key_hashed_idx = mmh3.hash(key, self._hash_seed[time]) % self._bit_num
            self.server.setbit(keyname, key_hashed_idx, 1)

        self._data_count += 1
        return True

    def is_exists(self, key):
        """
        :param key:
        :return:

        判断该值是否存在
        有任意一位为0 则肯定不存在
        """
        keyname = self.redis_key + str(sum(map(ord, key)) % self._block_num)

        for time in range(self._hash_num):
            key_hashed_idx = mmh3.hash(key, self._hash_seed[time]) % self._bit_num
            if not int(self.server.getbit(keyname, key_hashed_idx)):    # 类型？
                return False
        return True


if __name__ == '__main__':

    bf = RedisFilter()
    bf.start(100000000, 0.0001)
    # 仅测试输出，正常使用时透明
    print(bf._bit_num, bf._hash_num)
    print(bf._block_num)
    print(bf._bit_num)

    a = ['when', 'how', 'where', 'too', 'there', 'to', 'when']
    for i in a:
        print(bf.add(i))

    print('xixi in bf?: ', 'xixi' in bf)

    b = ['when', 'xixi', 'haha']
    for i in b:
        if bf.is_exists(i):
            print('%s exist' % i)
        else:
            print('%s not exist' % i)

    print('bf had load data: ', len(bf))

