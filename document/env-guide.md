---
title: 电商离线数仓 Windows 端环境配置
output: 
    word_document: default
---

## Hadoop Windows 端配置

所需文件

- hadoop-3.1.0
- hadoop-winutils
- temurin-1.8.0_432
- mysql-connector-j-8.0.33.jar

Apache hadoop-3.1.0 <https://archive.apache.org/dist/hadoop/common/hadoop-3.1.0/>

apache-hadoop-winutils <https://github.com/Zer0r3/winutils>

Eclipse Temurin JDK <https://adoptium.net/releases.html>

mysql-connector-j-8.0.33.jar <https://dev.mysql.com/downloads/connector/>

将所需文件解压到 `C:\Environment\JDK` 下，完成后该目录下应包含以下文件：

```plaintext
-hadoop-3.1.0
    -bin
        -hadoop.cmd
        -winutils.exe
        ...
    -etc
        -hadoop
            -core-site.xml
            -hdfs-site.xml
            -mapred-site.xml
            -yarn-site.xml
            ...
    -include
    -lib
    -libexec
    -logs
    -sbin
    -share
        -hadoop
            -common
                -lib
                -hadoop-common-3.1.0.jar
                ...
-temurin-1.8.0_432
    -bin
        -java.exe
        -javaw.exe
        ...
    -lib
        -tools.jar
        ...
```

`hadoop-env.sh` 编辑 `JAVA_HOME` 路径以及 `HADOOP_HOME` 路径

`hdfs-site.xml`

```xml
   <property>
       <name>dfs.replication</name>
       <value>1</value>
   </property>
   <property>
       <name>dfs.namenode.name.dir</name>
       <value>file:///C:/Environment/JDK/hadoop-3.1.0/specified_data/namenode</value>
   </property>
   <property>
       <name>dfs.datanode.data.dir</name>
       <value>file:///C:/Environment/JDK/hadoop-3.1.0/specified_data/datanode</value>
   </property>
   <property>
       <name>dfs.datanode.max.transfer.threads</name>
       <value>8192</value>
   </property>
```

> 同时建立 `C:\Environment\JDK\hadoop-3.1.0\specified_data\namenode`、 `C:\Environment\JDK\hadoop-3.1.0\specified_data\datanode`和`C:\Environment\JDK\hadoop-3.1.0\specified_data\tmp` 目录。

`core-site.xml`

```xml
  <property>
   <name>fs.defaultFS</name>
   <value>hdfs://localhost:9697</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>file:///C:/Environment/JDK/hadoop-3.1.0/specified_data/tmp</value>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>hadoop.http.staticuser.user</name>
    <value>Your username</value>
  </property>
```

> 这里的 root 改成你自己的用户名。查看 `C:\Users\` 下的用户名。

`sbin\start-dfs.cmd` 脚本部分最前面添加：

```cmd
cd C:\Environment\JDK\hadoop-3.1.0\bin
```

启动 dfs

使用终端管理员模式

```bash
cd C:\Environment\JDK\hadoop-3.1.0\
$env:JAVA_HOME="C:\Environment\JDK\temurin-1.8.0_432"   
$env:HADOOP_HOME="C:\Environment\JDK\hadoop-3.1.0"
$env:Path="$env:HADOOP_HOME\bin;$env:JAVA_HOME\bin;" + $env:Path
 
.\bin\hdfs namenode -format # 格式化 namenode，首次运行需要执行

winutils chmod -R 777 C:\Users\YourUsername\AppData\Local\Temp\

.\sbin\start-dfs.cmd
# localhost:9870
.\sbin\stop-dfs.cmd
```

> 一定要使用管理员模式运行，否则会报错。并且停止 dfs 时要使用 `.\sbin\stop-dfs.sh`，否则端口不会释放，需要重启电脑。

在管理页面，根目录下创建 `gmall` 目录（用于初始化，否则首次运行可能报错）

## Hive Windows 端配置

Hive 是构筑于 HDFS 上的，如果需要启用 Hive 需要确保 Hadoop 已经正确配置并启动。

所需文件

- hive-3.1.0
- Apache Hive 1.2.2 src

apache-hive-3.1.0-bin <https://archive.apache.org/dist/hive/hive-3.1.0/>

apache-hive-1.2.2-src <http://archive.apache.org/dist/hive/hive-1.2.2/>

将所需文件解压到 `C:\Environment\JDK` 下，将 `apache-hive-1.2.2-src` 解压后的 `bin` 目录下的所有文件复制到 `hive-3.1.0\bin` 目录下，选择覆盖同名文件，

完成后该目录下应包含以下文件：

```plaintext
-hive-3.1.0
    -bin
        -hive.cmd
        -hive.bat
        -schematool.cmd
        ...
    -conf
        -hive-site.xml
        ...
    -lib
    -logs
    -metastore
    -scripts
    -share
        -hive
            -common
            -jdbc
            ...
```

先在 HDFS 上创建 Hive 的仓库目录（在 Hadoop 目录下激活环境变量后进入 bin 目录运行）：

```bash
.\hdfs dfs -mkdir -p /user/hive/warehouse
.\hdfs dfs -chmod -R 777 /user/hive/warehouse
.\hdfs dfs -mkdir /tmp
.\hdfs dfs -chmod -R 777 /tmp
```

将 MySQL JDBC 驱动 `mysql-connector-j-8.0.33.jar` 复制到 `hive-3.1.0\lib` 目录下。

创建Hive的配置文件，在 conf 目录下已经有对应的配置文件模板，需要拷贝和重命名，具体如下：

hive-default.xml.template => **hive-site.xml**
hive-env.sh.template => hive-env.sh
hive-exec-log4j2.properties.template => hive-exec-log4j2.properties
hive-log4j2.properties.template => hive-log4j2.properties

编辑 `C:\Environment\JDK\hive-3.1.0\conf\hive-site.xml` 文件，修改以下内容：

```xml
<property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/user/hive/warehouse</value>
    <description>location of default database for the warehouse</description>
</property>
<property>
    <name>hive.exec.scratchdir</name>
    <value>/tmp/hive</value>
    <description>Scratch directory for Hive jobs</description>
</property>
<property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:mysql://localhost:3306/hive?useSSL=false&amp;allowPublicKeyRetrieval=true&amp;serverTimezone=Asia/Shanghai</value>
    <description>JDBC connect string for a JDBC metastore</description>
</property>
<property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>com.mysql.cj.jdbc.Driver</value>
    <description>Driver class name for a JDBC metastore</description>
</property>
<property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>hive</value>
    <description>username to use against metastore database</description>
</property>
<property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>hive</value>
    <description>password to use against metastore database</description>
</property>
<property>
    <name>hive.metastore.db.type</name>
    <value>MYSQL</value>
    <description>Type of database used for the metastore</description>
</property>
<property>
    <name>hive.exec.local.scratchdir</name>
    <value>C:\Environment\JDK\hive-3.1.0\data\scratchDir</value>
    <description>Local scratch space for Hive jobs</description>
</property>
<property>
    <name>hive.downloaded.resources.dir</name>
    <value>C:\Environment\JDK\hive-3.1.0\data\resourcesDir</value>
    <description>Local directory where Hive downloads resources</description>
</property>
<property>
    <name>hive.querylog.location</name>
    <value>C:\Environment\JDK\hive-3.1.0\data\querylogDir</value>
    <description>Local directory where Hive query logs are stored</description>
</property>
<property>
    <name>hive.server2.logging.operation.log.location</name>
    <value>C:\Environment\JDK\hive-3.1.0\data\operationDir</value>
    <description>Local directory where Hive operation logs are stored</description>
</property>
<property>
    <name>hive.metastore.schema.verification</name>
    <value>false</value>
    <description>Enables schema verification during metastore startup</description>
</property>
<property>
    <name>hive.server2.enable.doAs</name>
    <value>false</value>
    <description>Whether to execute Hive queries as the user who submitted them</description>
</property>
<property>
    <name>hive.execution.engine</name>
    <value>mr</value>
    <description>Execution engine: mr, tez, or spark</description>
</property>
<property>
    <name>mapreduce.framework.name</name>
    <value>local</value>
    <description>Use local mode for MapReduce without YARN</description>
</property>
<property>
    <name>hive.exec.mode.local.auto</name>
    <value>true</value>
    <description>Let Hive determine whether to run in local mode automatically</description>
</property>
```

> 选项 `hive.txn.xlock.iow` 的描述部分存在非法字符，删除即可。

建立目录：

```bash
mkdir C:\Environment\JDK\hive-3.1.0\data\scratchDir
mkdir C:\Environment\JDK\hive-3.1.0\data\resourcesDir
mkdir C:\Environment\JDK\hive-3.1.0\data\querylogDir
mkdir C:\Environment\JDK\hive-3.1.0\data\operationDir
```

需要提前配置 MySQL 数据库，创建 `hive` 用户，密码 `hive`，并创建必要的数据库。

### Hive 相关数据库说明

项目中涉及三个 MySQL 数据库：

1. **gmall**: 原始业务数据库（源数据）
   - 存储电商业务的原始数据
   - 包含订单、用户、商品等28张业务表
   - 数据来源：业务系统

2. **hive**: Hive 元数据存储数据库
   - 存储 Hive 表的元数据信息（表结构、分区等）
   - Hive 运行的必需数据库
   - 由 Hive metastore 服务使用

3. **gmall_hive**: Hive 模式结果数据库（仅在启用 Hive 支持时使用）
   - 存储 ADS 层的最终分析结果
   - 用于 Superset 可视化
   - 与 gmall 业务数据库分离，避免混淆

### 创建数据库和用户

```sql
-- 1. 创建 Hive 元数据存储数据库
CREATE DATABASE IF NOT EXISTS hive DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

-- 2. 创建 Hive 结果数据库（用于存储 ADS 层分析结果）
CREATE DATABASE IF NOT EXISTS gmall_hive DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

-- 3. 创建 hive 用户并授权
CREATE USER IF NOT EXISTS 'hive'@'localhost' IDENTIFIED WITH mysql_native_password BY 'hive';
GRANT ALL PRIVILEGES ON hive.* TO 'hive'@'localhost';
GRANT ALL PRIVILEGES ON gmall_hive.* TO 'hive'@'localhost';

-- 4. root 用户也需要访问 gmall_hive（用于 ADS 层写入）
GRANT ALL PRIVILEGES ON gmall_hive.* TO 'root'@'localhost';

FLUSH PRIVILEGES;
```

> **注意**：
>
> - `hive` 数据库用于 Hive 元数据存储，由 Hive 自动管理
> - `gmall_hive` 数据库用于存储数据仓库的最终分析结果
> - 在 `config/config.ini` 中配置这些数据库连接信息

修改hive-env.sh脚本，文件末尾添加以下内容：

```bash
export HADOOP_HOME=C:/Environment/JDK/hadoop-3.1.0
export JAVA_HOME=C:/Environment/JDK/temurin-1.8.0_432
export HIVE_CONF_DIR=C:/Environment/JDK/hive-3.1.0/conf
export HIVE_AUX_JARS_PATH=C:/Environment/JDK/hive-3.1.0/lib/mysql-connector-j-8.0.33.jar
```

替换 Hadoop 的 Guava 版本

```bash
# 备份 Hadoop 的旧 Guava
cd C:\Environment\JDK\hadoop-3.1.0\share\hadoop\common\lib
Copy-Item guava-*.jar guava-backup.jar.bak

# 删除 Hadoop 的旧 Guava（通常是 guava-11.0.2.jar）
Remove-Item guava-*.jar

# 假设 Hadoop 有 guava-11.0.2.jar，Hive 有 guava-19.0.jar
cd C:\Environment\JDK\hadoop-3.1.0\share\hadoop\common\lib
Remove-Item guava-11.0.2.jar -ErrorAction SilentlyContinue
Copy-Item "C:\Environment\JDK\hive-3.1.0\lib\guava-19.0.jar" .
```

配置终端环境变量

```bash
cd C:\Environment\JDK\hive-3.1.0\
$env:JAVA_HOME="C:\Environment\JDK\temurin-1.8.0_432"   
$env:HADOOP_HOME="C:\Environment\JDK\hadoop-3.1.0"
$env:Path="$env:HIVE_HOME\bin;$env:JAVA_HOME\bin;" + $env:Path
```

初始化 Hive 元数据库

```bash
cd C:\Environment\JDK\hive-3.1.0\bin
.\hive.cmd --service schematool -dbType mysql -initSchema --verbose
# 如果失败，检查配置是否正确
# MySQL 服务是否启动，清空 mysql 中的 hive 数据库中的数据或重建数据 
# 再输入以下指令后重试
Remove-Item -Path "C:\Environment\JDK\hive-3.1.0\metastore_db" -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -Path "C:\Environment\JDK\hive-3.1.0\derby.log" -Force -ErrorAction SilentlyContinue
```

连接 Hive

```bash
cd C:\Environment\JDK\hive-3.1.0\bin
hive.cmd
```

测试

```sql
create table t_test(id INT,name string);
show tables;
describe t_test;
-- 插入数据
insert into table t_test values(1,'zhangsan');
-- 查询数据
select * from t_test;
-- 删除表
drop table t_test;
```

## MySQL Windows 端配置

安装 mysql83 一切默认即可，账号 root 密码 123456，自定义即可

### 导入业务数据

安装 navicat 导入数据库 `.sql` 文件

> 不安装 navicat 则使用 mysql 命令行工具导入

```bash
mysql -u root -p123456 < C:\Download\gmall.sql
```

### 创建数据仓库相关数据库

```sql
-- 连接 MySQL
mysql -u root -p

-- 创建 Hive 元数据库
CREATE DATABASE IF NOT EXISTS hive DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

-- 创建 Hive 结果数据库（ADS 层输出）
CREATE DATABASE IF NOT EXISTS gmall_hive DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

-- 创建 hive 用户
CREATE USER IF NOT EXISTS 'hive'@'localhost' IDENTIFIED WITH mysql_native_password BY 'hive';
GRANT ALL PRIVILEGES ON hive.* TO 'hive'@'localhost';
GRANT ALL PRIVILEGES ON gmall_hive.* TO 'hive'@'localhost';

-- root 用户访问权限
GRANT ALL PRIVILEGES ON gmall_hive.* TO 'root'@'localhost';

FLUSH PRIVILEGES;
```

### 数据库用途说明

| 数据库 | 用途 | 使用者 | 说明 |
|--------|------|--------|------|
| `gmall` | 业务数据 | Spark/Hive | 原始业务数据，ODS 层数据源 |
| `hive` | 元数据存储 | Hive Metastore | Hive 表结构元数据 |
| `gmall_hive` | 分析结果 | ADS 层 | 最终分析指标，供 Superset 可视化 |
| `superset` | 可视化配置 | Superset | Superset 系统配置数据 |

## 复制 conda 环境

从其他电脑复制 conda 环境到本地

```bash
conda env export --name spark > spark_env.yml # 导出当前环境
```

然后将 `spark_env.yml` 文件复制到本地，执行以下命令创建环境：

```bash
conda env create --file spark_env.yml --name spark
conda activate spark
```

> 如果选择了复制 conda 环境，下面安装软件包的步骤可以跳过，直接使用复制的环境即可。其他照旧。

## Superset Windows 端配置

使用 docker 启动 superset（推荐方式）

安装 Docker Desktop，配置好 WSL2 环境(建议)。

以下步骤将安装中文版 Superset。

```bash
git clone https://github.com/lutinglt/superset-zh.git
cd superset-zh
docker build -t lutinglt/superset-zh .
docker run -d --name superset -p 8086:8088 lutinglt/superset-zh
docker exec -it superset superset fab create-admin --username admin --firstname 'admin' --lastname 'admin' --email admin@superset.apache.org --password 'admin'
docker exec -it superset superset db upgrade
docker exec -it superset superset init
docker exec -it superset superset load_examples
```

访问地址 <http://localhost:8086>

> 连不上MYSQL数据库？主机名是不是使用了 `localhost`/`127.0.0.1`？那就对了，需要使用 `host.docker.internal` 代替，或者创建虚拟网卡，使用该虚拟网卡的 IP 地址。第一次登陆后需要重启 superset。

---

以下步骤为手动安装 Superset 原版的步骤。

安装 superset 需要使用 conda 配置 python 3.10.8 环境

```bash
conda create -n superset-py39 python=3.9
conda activate superset-py39
pip install pillow ==11.2.1
pip install wheel==0.45.1
pip install python-geohash==0.8.5
pip install apache-superset==4.1.2
pip install gunicorn==23.0.0
pip install mysqlclient==2.2.7
pip install Flask==2.3.3
pip install Werkzeug==3.1.3
pip install marshmallow==3.26.1
pip install Flask-CORS
```

配置 superset `conda\superset-py39\Lib\site-packages\superset`

修改 `config.py`

```python
SQLALCHEMY_DATABASE_URI = 'mysql://superset:superset@localhost:3306/superset?charset=utf8'

SECRET_KEY = "i87mn1ga7zywzmgo0&c$%pt_6sn)^u6%)r45$r87n(o(#7&4sz"
```

> 需要提前配置 MySQL 数据库，创建 `superset` 用户，密码 `superset`，并创建数据库 `superset`。

```sql
CREATE DATABASE IF NOT EXISTS superset DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS 'superset'@'localhost' IDENTIFIED BY 'superset';
GRANT ALL PRIVILEGES ON superset.* TO 'superset'@'localhost';
FLUSH PRIVILEGES;
```

数据库可以选择性配置，如果不配置则使用 SQLite 数据库。密钥可以自定义。

```bash
set FLASK_APP = superset
superset db upgrade
superset fab create-admin # 创建管理员账号皆为 admin/admin 即可
superset init
```

> 如果 set FLASK_APP = superset 不生效，进入虚拟环境根目录执行其余步骤，忽略该步骤。同时创建 `app.py`，注意，这种方式每次都要进入虚拟环境根。

```python
import os
from superset.app import create_app
from flask_cors import CORS

# 设置 Superset 密钥
os.environ['SUPERSET_SECRET_KEY'] = "oh-so-secret"

# 设置其他环境变量来启用 CORS
os.environ['SUPERSET_WEBSERVER_TIMEOUT'] = "300"

app = create_app()

# 更强制性的 CORS 配置
CORS(app, 
     resources={
         r"/*": {
             "origins": "*",
             "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
             "allow_headers": ["Content-Type", "Authorization", "X-Requested-With", "Accept", "Origin"],
             "expose_headers": ["Content-Range", "X-Content-Range"],
             "supports_credentials": True,
             "max_age": 3600
         }
     })

# 添加额外的响应头处理
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Requested-With,Accept,Origin')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS,PATCH')
    response.headers.add('Access-Control-Allow-Credentials', 'true')
    return response

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8088)
```

启动 superset

```bash
superset load_examples # 加载示例数据，可以不做
superset run -p 8088 --with-threads --reload --debugger
# stop
superset stop
```

访问地址 <http://localhost:8088>

## spark 配置

安装 spark 需要使用 conda 配置 python 3.10.8 环境

```bash
conda create -n pyspark python=3.10.8
conda activate pyspark
pip install pyspark==3.3.1
pip install pandas==2.3.0
```

开发环境就使用该虚拟环境即可

## 提醒

启动开发环境前，将 HDFS、MYSQL 全部启动。

## 自动化启动环境

可以使用 `start-env-admin.ps1` 脚本来自动化启动环境，确保 HDFS 和 MySQL 服务在开发环境启动前已启动。

```powershell
# 启动 HDFS 和 MySQL 服务
.\start-env-admin.ps1
```

> Hive 和 Superset 需要在 HDFS 和 MySQL 启动后才能正常工作。

```powershell
# 启动 Hive 服务
.\start-hive-admin.ps1
```
