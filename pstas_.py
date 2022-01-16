#python profile
import pstats
# 创建 Stats 对象
p = pstats.Stats("process1.cprofile")
# 按照运行时间和函数名进行排序
#p.strip_dirs().sort_stats("cumulative", "name").print_stats(0.5)#参数为小数 表示前百分之几的函数信息
p.strip_dirs().sort_stats("cumulative", "name").print_stats(30)#参数为整数 打印前三十行
