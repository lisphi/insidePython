# from scipy.optimize import fsolve
# import numpy as np

# # f(x) = a * e^(b*x)
# # f(1) + f(2) + ... + f(32) = 7200

# #定义要解决的方程，变量x是一个数组，包含我们要找的两个未知数a和b
# def func(x):
#     a, b = x
#     c = a * np.exp(b)
#     total = sum([a * np.exp(b * i) for i in range(1, 33)]) - 7200
#     return [c - 1.5, total]

# # 为a和b设置一个初始猜测值
# x0 = [1, 1]

# # 调用fsolve()函数
# result = fsolve(func, x0)

# print(f"a: {result[0]}, b: {result[1]}")
