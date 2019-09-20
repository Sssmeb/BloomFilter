# -*- coding: utf-8 -*-
# @Time    : 2019/9/20 12:47
# @Author  : CRJ
# @File    : test.py
# @Software: PyCharm
# @Python3.6
def missingNumber(nums):
    if not nums:
        return
    nums += [-1]

    for idx, val in enumerate(nums):
        while nums[idx] != idx and nums[idx]!= -1:
            nums[idx], nums[val] = nums[val], nums[idx]

    for key, val in enumerate(nums):
        if val == -1:
            return key
missingNumber([9,6,4,2,3,5,7,0,1])
