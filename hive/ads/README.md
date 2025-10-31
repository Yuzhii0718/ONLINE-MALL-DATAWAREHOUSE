# Hive ADS 指标分类模块

此目录包含 Hive 模式下 ADS 层的分类指标实现。

## 模块说明

### 📊 hive_ads_orders.py - 订单相关指标

负责交易相关的统计分析：

| 方法 | 指标表 | 说明 |
|------|--------|------|
| `build_ads_trade_stats_by_tm()` | ads_trade_stats_by_tm | 品牌交易统计 |
| `build_ads_trade_stats_by_cate()` | ads_trade_stats_by_cate | 品类交易统计 |
| `build_ads_trade_stats_by_province()` | ads_trade_stats_by_province | 地区交易统计 |
| `build_ads_top_sku_by_total_num()` | ads_top_sku_by_total_num | 历史被下单总件数最多的商品（SKU Top1） |
| `build_ads_top_sku_by_province_7d()` | ads_top_sku_by_province_7d | 最近一周各省份下单件数最多的商品（各省Top1） |
| `build_ads_top3_sku_by_province()` | ads_top3_sku_by_province | 每个省份下单商品数量前三（各省Top3） |

**数据源**: `dws_trade_sku_order_td`, `dws_trade_province_order_td`, `dim_sku`, `dim_province`

**输出**: MySQL `gmall_hive` 数据库

### 👥 hive_ads_users.py - 用户相关指标

负责用户行为的统计分析：

| 方法 | 指标表 | 说明 |
|------|--------|------|
| `build_ads_user_stats()` | ads_user_stats | 用户总数和新增统计 |

**数据源**: `dwd_user_register`

**输出**: MySQL `gmall_hive` 数据库

### ❤️ hive_ads_favorites.py - 收藏相关指标

负责商品收藏的统计分析：

| 方法 | 指标表 | 说明 |
|------|--------|------|
| `build_ads_sku_favor_count_top10()` | ads_sku_favor_count_top10 | 商品收藏排行榜 Top10 |

**数据源**: `dwd_interaction_favor`, `dim_sku`

**输出**: MySQL `gmall_hive` 数据库

## 使用方式

### 方式1: 通过主入口调用（推荐）

```python
from hive.hive_dws_to_ads import HiveDWSToADS

# 执行所有ADS指标
ads = HiveDWSToADS()
ads.build_all_ads()
```

### 方式2: 单独调用某个模块

```python
from tools.spark_util import SparkUtil
from hive.ads.hive_ads_orders import HiveADSOrders

# 创建Spark会话
spark = SparkUtil.create_spark_session("ADS-Orders")

# 只执行订单相关指标
ads_orders = HiveADSOrders(spark)
ads_orders.build_ads_trade_stats_by_tm()
ads_orders.build_ads_trade_stats_by_cate()
ads_orders.build_ads_top_sku_by_total_num()
ads_orders.build_ads_top_sku_by_province_7d()
ads_orders.build_ads_top3_sku_by_province()
```

## 扩展指南

### 添加新指标到现有模块

例如，在订单模块添加"近7日热销商品"：

```python
# 在 hive_ads_orders.py 中添加
def build_ads_hot_sku_last7days(self):
    """构建近7日热销商品"""
    sql = """
    SELECT 
        dws.sku_id,
        ds.sku_name,
        SUM(dws.order_count) as order_count
    FROM dws_trade_sku_order_td dws
    INNER JOIN dim_sku ds ON dws.sku_id = ds.sku_id
    WHERE dws.dt >= date_sub(current_date(), 7)
    GROUP BY dws.sku_id, ds.sku_name
    ORDER BY order_count DESC
    LIMIT 20
    """
    
    result_df = self.spark.sql(sql)
    result_df.write \
        .format("jdbc") \
        .option("url", self.hive_mysql_url) \
        .option("dbtable", "ads_hot_sku_last7days") \
        .option("user", self.mysql_config['username']) \
        .option("password", self.mysql_config['password']) \
        .option("driver", self.mysql_config['driver']) \
        .mode("overwrite") \
        .save()
    
    return True
```

然后在 `hive_dws_to_ads.py` 中注册：

```python
def build_ads_hot_sku_last7days(self):
    """近7日热销商品（委托给订单指标类）"""
    return self.ads_orders.build_ads_hot_sku_last7days()

# 在 build_all_ads() 中添加任务
ads_tasks = [
    # ... 现有任务
    ("近7日热销商品", self.build_ads_hot_sku_last7days),
]
```

### 创建新的业务分类模块

参考 `document/ads_classify.md` 中的其他业务方向（如评论、退单等），创建新模块：

1. 创建新文件：`hive_ads_comments.py`
2. 定义类：`HiveADSComments`
3. 实现指标方法
4. 在 `hive_dws_to_ads.py` 中导入并初始化
5. 注册到 `build_all_ads()` 任务列表

## 设计原则

1. **单一职责**: 每个模块只负责一个业务方向
2. **独立性**: 模块间相互独立，可单独运行
3. **统一接口**: 所有指标方法返回 `True/False` 表示成功/失败
4. **配置统一**: 通过 `ConfigManager` 统一获取配置
5. **错误处理**: 每个方法都有异常捕获和日志记录

## 参考文档

- 指标分类规划: `../../document/ads_classify.md`
- Hive模式总览: `../README.md`
- Spark模式ADS参考: `../../x05_dws_to_ads/ads/`
