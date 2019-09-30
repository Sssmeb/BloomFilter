# -*- coding: utf-8 -*-
# @Time    : 2019/9/30 22:03
# @Author  : CRJ
# @File    : utils.py
# @Software: PyCharm
# @Python3.6
import uuid
import math
import time
import redis


# 获取锁
def acquire_lock_with_timeout(conn, lockname, acquire_timeout=5, lock_timeout=10):
    # 128位随机标识符
    identifier = str(uuid.uuid4())
    lockname = 'lock:' + lockname
    lock_timeout = int(math.ceil(lock_timeout))  # 确保传给exprie是整数

    end = time.time() + acquire_timeout
    while time.time() < end:
        if conn.setnx(lockname, identifier):
            conn.expire(lockname, lock_timeout)
            return identifier
        elif not conn.ttl(lockname):  # 为没有设置超时时间的锁设置超时时间
            conn.expire(lockname, lock_timeout)

        time.sleep(0.001)
    return False


# 释放锁
def release_lock(conn, lockname, identifier):
    pipe = conn.pipeline(True)
    lockname = 'lock:' + lockname

    while True:
        try:
            pipe.watch(lockname)
            # 判断标志是否相同
            if pipe.get(lockname) == identifier:
                pipe.multi()
                pipe.delete(lockname)
                pipe.execute()
                return True

            # 不同则直接退出 return False
            pipe.unwatch()
            break

        except redis.exceptions.WatchError:
            pass
    return False