"""
Spark工具类
提供Spark Session创建和通用操作
"""
import logging
from pyspark.sql import SparkSession
from pyspark import SparkConf
from config.config_manager import ConfigManager
from config.env_setup import setup_environment

class SparkUtil:
    _spark = None
    
    @classmethod
    def get_spark_session(cls, app_name="GMALL-DataWarehouse"):
        """获取Spark会话"""
        # 设置环境变量
        setup_environment()
        
        # 读取配置
        config_manager = ConfigManager()
        mysql_config = config_manager.get_mysql_config()
        spark_config = config_manager.get_spark_config()
        
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.driver.memory", spark_config['driver_memory']) \
            .config("spark.executor.memory", spark_config['executor_memory']) \
            .config("spark.driver.maxResultSize", spark_config['max_result_size']) \
            .config("spark.jars", mysql_config['jdbc_jar']) \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        return spark

    @classmethod
    def create_spark_session(cls, app_name=None):
        """创建Spark Session"""
        try:
            # 设置环境变量
            setup_environment()
            
            # 读取配置
            config_manager = ConfigManager()
            spark_config = config_manager.get_spark_config()
            mysql_config = config_manager.get_mysql_config()
            
            # 使用传入的app_name或配置文件中的默认值
            final_app_name = app_name if app_name else spark_config['app_name']
            
            # 创建Spark配置
            conf = SparkConf() \
                .setAppName(final_app_name) \
                .setMaster(spark_config['master']) \
                .set("spark.sql.warehouse.dir", spark_config['warehouse_dir']) \
                .set("spark.sql.adaptive.enabled", "true") \
                .set("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .set("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .set("spark.driver.memory", spark_config['driver_memory']) \
                .set("spark.executor.memory", spark_config['executor_memory']) \
                .set("spark.driver.maxResultSize", spark_config['max_result_size']) \
                .set("spark.jars", mysql_config['jdbc_jar'])
            
            # 创建Spark Session
            spark = SparkSession.builder \
                .config(conf=conf) \
                .enableHiveSupport() \
                .getOrCreate()
            
            # 设置日志级别
            spark.sparkContext.setLogLevel("WARN")
            
            logging.info(f"Spark Session创建成功: {final_app_name}")
            print(f"✓ Spark Session创建成功: {final_app_name}")
            print(f"  - Master: {spark_config['master']}")
            print(f"  - Driver Memory: {spark_config['driver_memory']}")
            print(f"  - Executor Memory: {spark_config['executor_memory']}")
            print(f"  - Max Result Size: {spark_config['max_result_size']}")
            print(f"  - Warehouse Dir: {spark_config['warehouse_dir']}")
            print(f"  - JDBC JAR: {mysql_config['jdbc_jar']}")
            
            return spark
            
        except Exception as e:
            logging.error(f"Spark Session创建失败: {e}")
            print(f"✗ Spark Session创建失败: {e}")
            raise
        
    @classmethod
    def read_from_mysql(cls, table_name, spark=None):
        """从MySQL读取数据"""
        if spark is None:
            spark = cls.get_spark_session()
        
        config_manager = ConfigManager()
        mysql_config = config_manager.get_mysql_config()
        mysql_url = config_manager.get_mysql_url()
        
        try:
            df = spark.read \
                .format("jdbc") \
                .option("url", mysql_url) \
                .option("dbtable", table_name) \
                .option("user", mysql_config['username']) \
                .option("password", mysql_config['password']) \
                .option("driver", mysql_config['driver']) \
                .load()
            
            print(f"✓ 从MySQL读取表 {table_name} 成功，记录数: {df.count()}")
            return df
            
        except Exception as e:
            logging.error(f"从MySQL读取表 {table_name} 失败: {e}")
            print(f"✗ 从MySQL读取表 {table_name} 失败: {e}")
            raise
    
    @classmethod
    def write_to_mysql(cls, df, table_name, mode="overwrite", spark=None):
        """写入数据到MySQL"""
        if spark is None:
            spark = cls.get_spark_session()
        
        config_manager = ConfigManager()
        mysql_config = config_manager.get_mysql_config()
        mysql_url = config_manager.get_mysql_url()
        
        try:
            df.write \
                .format("jdbc") \
                .option("url", mysql_url) \
                .option("dbtable", table_name) \
                .option("user", mysql_config['username']) \
                .option("password", mysql_config['password']) \
                .option("driver", mysql_config['driver']) \
                .mode(mode) \
                .save()
            
            print(f"✓ 写入MySQL表 {table_name} 成功，记录数: {df.count()}")
            
        except Exception as e:
            logging.error(f"写入MySQL表 {table_name} 失败: {e}")
            print(f"✗ 写入MySQL表 {table_name} 失败: {e}")
            raise
    
    @classmethod
    def read_from_hdfs(cls, path, format="parquet", spark=None):
        """从HDFS读取数据"""
        if spark is None:
            spark = cls.get_spark_session()
        
        try:
            df = spark.read.format(format).load(path)
            print(f"✓ 从HDFS读取数据成功: {path}，记录数: {df.count()}")
            return df
            
        except Exception as e:
            logging.error(f"从HDFS读取数据失败: {path}, {e}")
            print(f"✗ 从HDFS读取数据失败: {path}, {e}")
            raise
    
    @classmethod
    def write_to_hdfs(cls, df, path, format="parquet", mode="overwrite", spark=None):
        """写入数据到HDFS"""
        if spark is None:
            spark = cls.get_spark_session()
        
        try:
            df.write \
                .format(format) \
                .mode(mode) \
                .save(path)
            
            print(f"✓ 写入HDFS成功: {path}，记录数: {df.count()}")
            
        except Exception as e:
            logging.error(f"写入HDFS失败: {path}, {e}")
            print(f"✗ 写入HDFS失败: {path}, {e}")
            raise
    
    @classmethod
    def stop_spark_session(cls):
        """停止Spark Session"""
        if cls._spark is not None:
            cls._spark.stop()
            cls._spark = None
            print("✓ Spark Session已停止")
