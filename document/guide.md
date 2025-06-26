---
title: 电商离线数仓 Windows 端环境配置
output: 
    word_document: default
---

## hadoop windows 端配置

所需文件

- 已经配置好的 hadoop-3.1.0 压缩包
- temurin-1.8.0_432

apache-hadoop-3.1.0-winutils <https://github.com/s911415/apache-hadoop-3.1.0-winutils/tree/master/bin>

Apache Archive Distribution Directory <https://archive.apache.org/dist/hadoop/common/hadoop-3.1.0/>

将所需文件解压到 `C:\Environment\JDK` 下，完成后该目录下应包含以下文件：

```plaintext
-hadoop-3.1.0
    -bin
        -hadoop.cmd
        -winutils.exe
    -etc
        -hadoop
        -core-site.xml
        -hdfs-site.xml
        -mapred-site.xml
        -yarn-site.xml
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
    -lib
        -tools.jar
```

编辑 `C:\Environment\JDK\hadoop-3.1.0\etc\hadoop\core-site.xml` 文件，添加以下内容：

```xml
    <property>
        <name>hadoop.http.staticuser.user</name>
        <value>root</value>
    </property>
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
       <value>C:\Environment\JDK\hadoop-3.1.0\specified_data\namenode</value>
   </property>
   <property>
       <name>dfs.datanode.data.dir</name>
       <value>C:\Environment\JDK\hadoop-3.1.0\specified_data\datanode</value>
   </property>
```

> 同时建立 `C:\Environment\JDK\hadoop-3.1.0\specified_data\namenode` 和 `C:\Environment\JDK\hadoop-3.1.0\specified_data\datanode` 目录。

`core-site.xml`

```xml
  <property>
   <name>fs.defaultFS</name>
   <value>hdfs://localhost:9697</value>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>hadoop.http.staticuser.user</name>
    <value>root</value>
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
cd ..\..\Environment\JDK\hadoop-3.1.0\
$env:Path = "C:\Environment\JDK\hadoop-3.1.0\bin" + $env:Path
$env:JAVA_HOME="C:\Environment\JDK\temurin-1.8.0_432"
$env:HADOOP_HOME="C:\Environment\JDK\hadoop-3.1.0"

.\bin\hdfs namenode -format # 格式化 namenode，首次运行需要执行

.\sbin\start-dfs.cmd
# localhost:9870
.\sbin\stop-dfs.cmd
```

> 一定要使用管理员模式运行，否则会报错。并且停止 dfs 时要使用 `.\sbin\stop-dfs.sh`，否则端口不会释放，需要重启电脑。

在管理页面，根目录下创建 `gmall` 目录（用于初始化，否则首次运行可能报错）

## mysql windows 端配置

安装 mysql83 一切默认即可，账号 root 密码 123456，自定义即可

安装 navicat 导入数据库 `.sql` 文件

> 不安装 navicat 则使用 mysql 命令行工具导入

```bash
mysql -u root -p123456 < C:\Download\gmall.sql
```

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

## superset windows 端配置

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
