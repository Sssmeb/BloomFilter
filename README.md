

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



接口同上。



## 问题

如果出现

> OOM command not allowed when used memory > 'maxmemory'

是redis默认的maxmemory限制了最大内存，在配置文件中修改参数即可。