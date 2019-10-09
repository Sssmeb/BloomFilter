
布隆过滤器相关知识详解：https://www.jianshu.com/p/e4773b69319d

分布式锁相关实现：https://www.jianshu.com/p/cf311cfb1689

文件说明：
- BloomFilter ： 基于单机内存的普通布隆过滤器。
- RedisFilter ： 基于redis实现的分布式布隆过滤器（含分布式锁，可应用于scrapyredis持久化去重）。
- scrapyredis ： scrapyredis库，用分布式布隆过滤器实现去重功能。

# BloomFilter
BloomFilter Based on py3(基于py3的布隆过滤器)

document：

```
# 初始化
# 通过这两个参数来确定需要多少个哈希函数以及位数组的大小
>>> bf = BloomFilter(data_size=100000, error_rate=0.001)

# 添加进布隆过滤器
>>> bf.add("test")
True

# 可以用in或者is_exists方法判断
>>> "test" in b
True
>>> bf.is_exists("test")
True
>>> bf.is_exists("test2")
False


# copy方法获取一个一模一样的实例
new_bf = bf.copy()
>>> new_bf.is_exists("test")
True
>>> new_bf.is_exists("test2")
False

```





# RedisFilter

用redis实现布隆过滤器，可用于scrapy-redis实现持久化去重。具体操作可参照：https://www.jianshu.com/p/e4773b69319d


## 为什么需要分布式锁？
在高并发的场景下，可能出现一个客户端在进行add操作的同时，另一个客户端在进行is_exists，有可能出现错判的情况。

此时能采用的方法有：
1. 事务。使用watch、multi、exec等。 
2. 非事务型流水线。（将一系列命令打包再发送给redis）
3. 分布式锁。

三种方法的优劣分别是：
1. 采用事务实现操作简单，只需要在原代码头尾加上事务相关的代码即可。但事务有一个明显的缺点是乐观锁带来失败重试导致效率降低，在本场景下不明显，但是例如商品秒杀活动场景，乐观锁会造成大量的失败重试。
2. 非事务型流水线。（由于布隆过滤器是操作位数组，需要循环发送逐位操作。即一次add或exist操作实际上是要通过多条命令、对多个位进行操作）。能解决普通分布式过滤器的需求，但是对于scrapyredis的去重过滤仍有可能出错（需要结合源码）
3. 能保证数据的安全性。但实现相对麻烦。

所以我们采用 分布式锁。

分布式锁原理分析及实现：https://www.jianshu.com/p/cf311cfb1689

## 问题

如果出现

> OOM command not allowed when used memory > 'maxmemory'

是redis默认的maxmemory限制了最大内存，在配置文件中修改参数即可。
