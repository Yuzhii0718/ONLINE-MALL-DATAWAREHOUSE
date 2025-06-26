# 工具类模块

此目录包含项目中使用的通用工具类。

## 文件说明

- `spark_util.py`: Spark工具类，提供Spark Session管理和通用数据操作方法

## 使用方法

```python
from tools.spark_util import SparkUtil

# 获取Spark Session
spark = SparkUtil.get_spark_session("MyApp")

# 从MySQL读取数据
df = SparkUtil.read_from_mysql("table_name")

# 写入数据到HDFS
SparkUtil.write_to_hdfs(df, "hdfs://path/to/save")

# 停止Spark Session
SparkUtil.stop_spark_session()
```

## 扩展

可以在此目录下添加其他通用工具类，如：
- 数据质量检查工具
- 数据转换工具
- 监控工具等
