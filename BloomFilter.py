# -*- coding: utf-8 -*-
# @Time    : 2019/9/18 20:59
# @Author  : CRJ
# @File    : BloomFilter.py
# @Software: PyCharm
# @Python3.6
import math
try:
    import bitarray
except ImportError:
    raise ImportError('Requires bitarray')

try:
    import mmh3
except ImportError:
    raise ImportError('Requires mmh3')


class BloomFilter(object):

    def __init__(self, data_size, error_rate=0.001):
        """

        :param data_size: 所需存放数据的数量
        :param error_rate:  可接受的误报率，默认0.001

        通过这两个参数来确定需要多少个哈希函数以及位数组的大小
        """

        if not data_size > 0:
            raise ValueError("Capacity must be > 0")
        if not (0 < error_rate < 1):
            raise ValueError("Error_Rate must be between 0 and 1.")

        self.data_size = data_size
        self.error_rate = error_rate

        bit_num, hash_num = self._adjust_param(data_size, error_rate)
        self._bit_array = bitarray.bitarray(bit_num)
        self._bit_array.setall(0)
        self._bit_num = bit_num
        self._hash_num = hash_num

        # 将哈希种子固定为 1 - hash_num （预留持久化过滤的可能）
        self._hash_seed = [i for i in range(1, hash_num+1)]

        # 已存数据量
        self._data_count = 0

    def add(self, key):
        if self._is_half_fill():
            raise IndexError("The capacity is insufficient")

        for time in range(self._hash_num):
            key_hashed_idx = mmh3.hash(key, self._hash_seed[time]) % self._bit_num
            self._bit_array[key_hashed_idx] = 1

        self._data_count += 1
        return True

    def is_exists(self, key):
        """
        判断该值是否存在
        有任意一位为0 则肯定不存在

        :param key:
        :return:
        """

        for time in range(self._hash_num):
            key_hashed_idx = mmh3.hash(key, self._hash_seed[time]) % self._bit_num
            if not self._bit_array[key_hashed_idx]:
                return False
        return True

    def copy(self):
        """

        复制一份布隆过滤器的实例
        :return:
        """
        new_filter = BloomFilter(self.data_size, self.error_rate)
        return self._copy_param(new_filter)

    def _copy_param(self, filter):
        filter._bit_array = self._bit_array
        filter._bit_num = self._bit_num
        filter._hash_num = self._hash_num
        filter._hash_seed = self._hash_seed
        filter._data_count = self._data_count
        return filter

    def _is_half_fill(self):
        """
        判断数据是否已经超过容量的一半
        :return:
        """
        return self._data_count > (self._hash_num // 2)

    def _adjust_param(self, data_size, error_rate):
        """
        :param data_size:
        :param error_rate:
        :return:

        通过数据量和期望的误报率 计算出 位数组大小 和 哈希函数的数量
        k为哈希函数个数    m为位数组大小
        n为数据量          p为误报率
        m = - (nlnp)/(ln2)^2

        k = (m/n) ln2
        """
        p = error_rate
        n = data_size
        m = - (n * (math.log10(p)) / (math.log10(2))**2)
        k = m / n * math.log10(2)

        return int(m), int(k)

    def __len__(self):
        """"
        返回现有数据容量
        """
        return self._data_count

    def __contains__(self, key):
        """

        :param key:
        :return:

        用于实现 in 判断
        """
        return self.is_exists(key)


if __name__ == '__main__':
    bf = BloomFilter(10000000, 0.0001)
    print(bf._bit_num, bf._hash_num)

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


    new_bf = bf.copy()
    print('new_bf had load data: ', len(bf))