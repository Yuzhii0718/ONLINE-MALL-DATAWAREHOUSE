# GMALL电商离线数据仓库

## 项目概述

这是一个基于 PySpark 3.1.0的电商离线数据仓库项目，采用分层架构设计，实现从业务数据到可视化展示的完整数据流处理。

## 技术架构

- **计算引擎**: PySpark 3.1.0
- **存储系统**: HDFS、MySQL
- **可视化**: Superset
- **开发语言**: Python
- **运行环境**: Windows 10/11、Linux

> 互联网上大多流传了 HIVE+FLUME+HBASE+SPARK+HADOOP+SUPERSET+KAFKA 的架构，但本项目使用了更轻量级架构配置，同时开源代码，可在 Windows 10/11 上运行，适合研究和学习离线数据仓库的基本原理和实现。

## 数据分层架构

```plaintext
MySQL业务数据
    ↓
ODS层（原始数据层）
    ↓
DIM层（维度数据层） + DWD层（明细数据层）
    ↓
DWS层（轻度聚合层）
    ↓
ADS层（应用数据服务层）
    ↓
MySQL结果数据 → Superset可视化
```

## 项目结构

```plaintext
GMALL-Data-Warehouse/
├── config/                      # 配置文件目录
│   ├── config.ini              # 主配置文件
│   ├── config_manager.py       # 配置管理器
│   └── env_setup.py           # 环境变量设置
├── document/                   # 文档目录
├── tools/                      # 工具类目录
│   └── spark_util.py          # Spark工具类
├── x01_mysql_to_ods/           # MySQL到ODS层
│   └── mysql_to_ods.py        # 数据同步脚本
├── x02_ods_to_dim/             # ODS到DIM维度表
│   └── ods_to_dim.py          # 维度表构建脚本
├── x03_ods_to_dwd/             # ODS到DWD事实表
│   └── ods_to_dwd.py          # 事实表构建脚本
├── x04_dwd_to_dws/             # DWD到DWS轻度聚合
│   └── dwd_to_dws.py          # 轻度聚合脚本
├── x05_dws_to_ads/             # DWS到ADS应用数据服务
│   └── dws_to_ads.py          # 应用数据构建脚本
├── logs/                       # 日志目录
├── main.py                     # 主运行脚本
├── requirements.txt            # Python依赖
└── README.md                   # 项目说明文档
```

## 环境配置

推荐配置：

- CPU: 8 cores@ 3.0 GHz
- 内存: 16 GB and above
- 磁盘: at least 10 GB free space SSD

### 1. Python环境要求

- Python 3.8+
- PySpark 3.1.0

或使用参考环境，见下文。

### 2. 外部依赖

- Hadoop 3.1.0
- Java 8
- MySQL 8.3
- Superset

### 3. 配置文件设置

编辑 `config/config.ini` 文件，配置以下信息：

```ini
[mysql]
host = localhost
port = 3306
database = gmall
username = root
password = 123456
driver = com.mysql.cj.jdbc.Driver
jdbc_jar = jars/mysql-connector-j-8.0.33.jar

[hdfs]
namenode = hdfs://localhost:9697
user = root

[spark]
app_name = GMALL-DataWarehouse
master = local[*]
warehouse_dir = hdfs://localhost:9697/spark-warehouse
driver_memory = 8g
executor_memory = 8g
max_result_size = 4g

[paths]
ods_path = hdfs://localhost:9697/gmall/ods
dwd_path = hdfs://localhost:9697/gmall/dwd
dim_path = hdfs://localhost:9697/gmall/dim
dws_path = hdfs://localhost:9697/gmall/dws
ads_path = hdfs://localhost:9697/gmall/ads

[environment]
hadoop_home = C:\Environment\JDK\hadoop-3.1.0
java_home = C:\Environment\JDK\temurin-1.8.0_432
pyspark_python = D:\Environment\Python\conda\pyspark310\python.exe
pyspark_driver_python = D:\Environment\Python\conda\pyspark310\python.exe
```

> 源码中不包含 `mysql-connector-j-8.0.33.jar`，请自行下载并放置在 `jars/` 目录下。根据自己环境选择合适的MySQL JDBC驱动版本。

## 安装与运行

### 1. 安装Python依赖

```bash
conda env create -f environment.yml
```

### 2. 启动Hadoop和MySQL服务

确保以下服务正常运行：

- Hadoop HDFS
- MySQL数据库服务

### 3. 运行数据仓库流水线

#### 完整流水线执行

```bash
python main.py
```

#### 单步骤执行

```bash
# MySQL到ODS层数据同步
python main.py mysql_to_ods

# ODS到DIM维度表构建
python main.py ods_to_dim

# ODS到DWD事实表构建
python main.py ods_to_dwd

# DWD到DWS轻度聚合
python main.py dwd_to_dws

# DWS到ADS应用数据服务
python main.py dws_to_ads
```

## 导入 MySQL 数据

存在 document 中，`.sql` 文件，包含 MySQL 数据库的表结构和示例数据。

## 配置 Superset/MySQL/Hadoop

[guide.md](./document/guide.md) 中有详细的配置说明。

## 数据层说明

### ODS层（原始数据层）

- **目的**: 原始数据存储，保持数据源的原始格式
- **存储**: HDFS Parquet格式
- **来源**: MySQL业务数据库
- **表数量**: 28张业务表

### DIM层（维度数据层）

- **目的**: 维度数据管理，为事实表提供维度信息
- **包含维度**: 用户维度、商品维度、日期维度、优惠券维度、活动维度
- **存储**: HDFS Parquet格式

### DWD层（明细数据层）

- **目的**: 事务事实数据存储
- **主要事实表**:
  - 交易域下单事务事实表
  - 交易域支付成功事务事实表
  - 交易域加购事务事实表
  - 工具域优惠券使用事务事实表
  - 互动域收藏商品事务事实表
  - 用户域用户注册事务事实表
- **存储**: HDFS Parquet格式

### DWS层（轻度聚合层）

- **目的**: 中间指标计算，减少重复计算
- **聚合粒度**: 用户粒度、商品粒度、地区粒度等
- **存储**: HDFS Parquet格式

### ADS层（应用数据服务层）

- **目的**: 最终业务指标计算
- **主要指标**:
  - 各品牌商品交易统计
  - 各分类商品交易统计
  - 各省份交易统计
  - 用户统计
  - 商品收藏排行榜TOP10
  - 优惠券使用统计
- **存储**: MySQL数据库

## 业务指标体系

### 交易主题

- 订单数、订单金额
- 用户数、商品数
- 活动减免金额、优惠券减免金额

### 用户主题

- 总用户数、下单用户数
- 用户登录情况
- 用户收藏行为

### 商品主题

- 商品销售排行
- 商品收藏排行
- 分类销售统计

### 营销主题

- 优惠券使用统计
- 活动效果分析

## 可视化展示

使用Superset连接MySQL的ADS层表进行可视化展示：

1. 访问 <http://127.0.0.1:8088>
2. 使用 admin/admin 登录
3. 配置MySQL数据源
4. 创建图表和仪表板

## 性能优化

### Spark优化配置

- 自适应查询执行 (AQE)
- 动态分区合并
- Kryo序列化
- Arrow向量化

### 存储优化

- Parquet列式存储
- 分区存储策略
- 数据压缩

## 开发规范

1. **配置管理**: 所有配置参数从配置文件读取
2. **错误处理**: 完善的异常捕获和错误日志
3. **代码结构**: 模块化设计，便于维护
4. **性能监控**: 记录处理时间和数据量
5. **数据质量**: 数据校验和质量检查

## 故障排除

### 常见问题

1. **Spark启动失败**
   - 检查Java环境变量
   - 检查Hadoop环境变量
   - 确认端口没有被占用

2. **HDFS连接失败**
   - 检查Hadoop服务状态
   - 确认HDFS端口配置正确

3. **MySQL连接失败**
   - 检查MySQL服务状态
   - 确认连接参数正确
   - 检查网络连通性

### 日志查看

- 应用日志：`logs/gmall_datawarehouse.log`
- Spark日志：Spark Web UI
- Hadoop日志：Hadoop日志目录

## 联系信息

如有问题，请检查日志文件或联系开发者。

---

**版本**: 1.0.0  
**更新时间**: 2025-06-23  
**开发者**: Medici

课设作品，没有长期维护计划。
