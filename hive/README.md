# Hive 脚本目录

此目录包含基于 Hive 的数据仓库处理脚本。

## 目录结构

```plaintext
hive/
├── __init__.py                    # Python包初始化
├── README.md                      # 本文件
├── hive_mysql_to_ods.py          # MySQL到ODS层同步
├── hive_ods_to_dim.py            # ODS到DIM维度表构建
├── hive_ods_to_dwd.py            # ODS到DWD事实表构建
├── hive_dwd_to_dws.py            # DWD到DWS轻度聚合
├── hive_dws_to_ads.py            # DWS到ADS应用指标构建（主入口）
└── ads/                           # ADS层分类指标模块
    ├── __init__.py
    ├── hive_ads_orders.py        # 订单相关指标（品牌/品类/地区交易统计）
    ├── hive_ads_users.py         # 用户相关指标（用户统计）
    └── hive_ads_favorites.py     # 收藏相关指标（商品收藏Top10）
```

## 模块架构

### ADS 层指标分类

ADS 层采用模块化设计，每个业务方向独立成文件：

| 模块 | 文件 | 指标 |
|------|------|------|
| 订单相关 | `ads/hive_ads_orders.py` | 品牌交易统计、品类交易统计、地区交易统计 |
| 用户相关 | `ads/hive_ads_users.py` | 用户总数统计、新增用户统计 |
| 收藏相关 | `ads/hive_ads_favorites.py` | 商品收藏Top10排行榜 |

### 指标调用流程

```plaintext
hive_dws_to_ads.py (主入口)
    ├── HiveADSOrders (订单指标类)
    │   ├── build_ads_trade_stats_by_tm()
    │   ├── build_ads_trade_stats_by_cate()
    │   └── build_ads_trade_stats_by_province()
    ├── HiveADSUsers (用户指标类)
    │   └── build_ads_user_stats()
    └── HiveADSFavorites (收藏指标类)
        └── build_ads_sku_favor_count_top10()
```

## 配置说明

在 `config/config.ini` 中配置：

```ini
[hive]
enable_hive_support = true           # 启用Hive模式
exec_mysql_database = gmall_hive     # Hive结果数据专用MySQL数据库

[mysql]
# ... MySQL配置（用于读取源数据）

[environment]
hive_home = C:\Environment\JDK\hive-3.1.0  # Hive安装目录
```

### 数据库说明

- **gmall**: 原始业务数据库（MySQL源数据）
- **gmall_hive**: Hive模式结果数据库（ADS层输出到此数据库）
- **hive**: Hive元数据存储数据库

## 使用说明

### 启用 Hive 模式

```bash
# 方式1: 通过main.py自动检测（推荐）
python main.py

# 方式2: 直接运行hive.py
python hive.py

# 单步执行
python main.py mysql_to_ods    # 自动使用Hive模式
python hive.py dws_to_ads       # 直接指定Hive模式
```

### 数据流程

```plaintext
MySQL (gmall)
    ↓
Hive ODS层 (ods_*)
    ↓
Hive DIM层 (dim_*) + Hive DWD层 (dwd_*)
    ↓
Hive DWS层 (dws_*)
    ↓
Hive ADS层 → MySQL (gmall_hive)
    ↓
Superset可视化
```

## 已实现功能

### 1. MySQL到ODS (hive_mysql_to_ods.py)

- 使用Spark enableHiveSupport
- 从MySQL读取28张业务表
- 创建Hive ODS表 (ods_*)
- 完整数据同步

### 2. ODS到DIM (hive_ods_to_dim.py)

- 日期维度表 (dim_date): 2020-2030年日期数据
- 用户维度表 (dim_user): 用户基本信息
- 商品SKU维度表 (dim_sku): 商品+品牌+品类
- 地区维度表 (dim_province): 省份+区域

### 3. ODS到DWD (hive_ods_to_dwd.py)

- 交易域下单事务 (dwd_trade_order_detail)
- 交易域支付事务 (dwd_trade_pay_detail)
- 用户域注册事务 (dwd_user_register)
- 互动域收藏事务 (dwd_interaction_favor)

### 4. DWD到DWS (hive_dwd_to_dws.py)

- 用户粒度订单汇总 (dws_trade_user_order_td)
- 商品粒度订单汇总 (dws_trade_sku_order_td)
- 地区粒度订单汇总 (dws_trade_province_order_td)
- 用户行为汇总 (dws_user_action_td)

### 5. DWS到ADS (hive_dws_to_ads.py)

采用**模块化分类设计**，输出到 MySQL `gmall_hive` 数据库：

#### 订单相关指标 (hive_ads_orders.py)

- ads_trade_stats_by_tm: 品牌交易统计
- ads_trade_stats_by_cate: 品类交易统计  
- ads_trade_stats_by_province: 地区交易统计

#### 用户相关指标 (hive_ads_users.py)

- ads_user_stats: 用户总数和新增统计

#### 收藏相关指标 (hive_ads_favorites.py)

- ads_sku_favor_count_top10: 商品收藏排行榜Top10

> **扩展说明**: 可参考 `document/ads_classify.md` 了解更多指标分类建议，如评论、退单等业务方向

## 技术实现

### 核心技术

- **Spark SQL** with enableHiveSupport
- **HiveQL**: 使用SQL进行数据转换
- **JDBC**: 连接MySQL读写数据
- **Parquet**: Hive默认存储格式

### 关键代码模式

```python
# 创建支持Hive的Spark会话
spark = SparkSession.builder \
    .enableHiveSupport() \
    .getOrCreate()

# 从MySQL读取
df = spark.read.format("jdbc") \
    .option("url", mysql_url) \
    .option("dbtable", table_name) \
    .load()

# 创建Hive表
df.createOrReplaceTempView("temp_table")
spark.sql("CREATE TABLE hive_table AS SELECT * FROM temp_table")

# 写回MySQL (ADS层)
result_df.write.format("jdbc") \
    .option("url", hive_mysql_url) \
    .option("dbtable", "ads_table") \
    .mode("overwrite") \
    .save()
```

## 前置条件

1. **Hive服务已启动**

   ```powershell
   .\start-hive-admin.ps1
   ```

2. **HDFS服务已启动**

   ```powershell
   .\start-env-admin.ps1
   ```

3. **MySQL数据库已准备**
   - gmall: 源业务数据
   - hive: Hive元数据存储
   - gmall_hive: ADS结果数据（需手动创建）

   ```sql
   CREATE DATABASE IF NOT EXISTS gmall_hive 
   DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
   ```

4. **Python环境**
   - PySpark 3.1.0+
   - Conda环境: pyspark310

## 性能优化

- 使用 Hive 分区表（后续可优化）
- 启用 Spark SQL 自适应查询
- 合理设置 Driver/Executor 内存
- 使用 Parquet 列式存储

## 监控和调试

### 查看Hive表

```sql
# 连接Hive
hive

# 查看所有表
SHOW TABLES;

# 查看表结构
DESCRIBE dim_user;

# 查询数据
SELECT * FROM ods_user_info LIMIT 10;
```

### 查看MySQL结果

```sql
USE gmall_hive;
SHOW TABLES;
SELECT * FROM ads_trade_stats_by_tm;
```

### 日志

- 查看项目日志: `logs/gmall_datawarehouse.log`
- Hive日志: `$HIVE_HOME/logs/`
- Hadoop日志: `$HADOOP_HOME/logs/`

## 注意事项

- Hive表名使用下划线命名（snake_case）
- 时间字段使用字符串类型存储
- ADS结果表需在gmall_hive数据库中预先创建或允许JDBC自动创建
- 首次运行会创建所有表，后续运行会覆盖数据
- 确保Spark有足够内存处理大数据量

## 故障排除

### 问题1: 找不到Hive表

```plaintext
Table or view not found: ods_user_info
```

**解决**: 先运行 MySQL到ODS 步骤，确保ODS表已创建

### 问题2: JDBC连接失败

```plaintext
Access denied for user 'root'@'localhost'
```

**解决**: 检查config.ini中的MySQL配置和密码

### 问题3: 内存不足

```plaintext
Java heap space
```

**解决**: 增加config.ini中的 `driver_memory` 和 `executor_memory`

### 问题4: Hive元数据错误

```plaintext
MetaException
```

**解决**: 检查Hive metastore服务和MySQL hive数据库

## 扩展开发

### 添加新的ADS指标

1. 在 `hive_dws_to_ads.py` 中添加新方法
2. 编写HiveQL查询
3. 将结果写入MySQL
4. 在 `build_all_ads()` 中注册新任务

示例：

```python
def build_ads_new_metric(self):
    sql = """
    SELECT ...
    FROM dws_table
    GROUP BY ...
    """
    result_df = self.spark.sql(sql)
    result_df.write.jdbc(...)
```
