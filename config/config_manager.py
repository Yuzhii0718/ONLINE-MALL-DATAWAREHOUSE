"""
配置读取工具类
统一管理配置文件的读取
"""
import configparser
import os
import logging

class ConfigManager:
    def __init__(self, config_file='config/config.ini'):
        """初始化配置管理器"""
        self.config_file = config_file
        self.config = configparser.ConfigParser()
        self.load_config()
    
    def load_config(self):
        """加载配置文件"""
        try:
            if not os.path.exists(self.config_file):
                raise FileNotFoundError(f"配置文件不存在: {self.config_file}")
            
            self.config.read(self.config_file, encoding='utf-8')
            logging.info(f"配置文件加载成功: {self.config_file}")
            print(f"✓ 配置文件加载成功: {self.config_file}")
            
        except Exception as e:
            logging.error(f"配置文件加载失败: {e}")
            raise
    
    def get_mysql_config(self):
        """获取MySQL配置"""
        return {
            'host': self.config.get('mysql', 'host'),
            'port': int(self.config.get('mysql', 'port')),
            'database': self.config.get('mysql', 'database'),
            'username': self.config.get('mysql', 'username'),
            'password': self.config.get('mysql', 'password'),
            'driver': self.config.get('mysql', 'driver'),
            'jdbc_jar': self.config.get('mysql', 'jdbc_jar')
        }
    
    def get_mysql_jdbc_jar(self):
        """获取MySQL JDBC JAR路径"""
        return self.config.get('mysql', 'jdbc_jar')
    
    def get_hdfs_config(self):
        """获取HDFS配置"""
        return {
            'namenode': self.config.get('hdfs', 'namenode'),
            'user': self.config.get('hdfs', 'user')
        }
    
    def get_spark_config(self):
        """获取Spark配置"""
        return {
            'app_name': self.config.get('spark', 'app_name'),
            'master': self.config.get('spark', 'master'),
            'warehouse_dir': self.config.get('spark', 'warehouse_dir'),
            'driver_memory': self.config.get('spark', 'driver_memory', fallback='2g'),
            'executor_memory': self.config.get('spark', 'executor_memory', fallback='2g'),
            'max_result_size': self.config.get('spark', 'max_result_size', fallback='1g')
        }

    def get_hive_config(self):
        """获取Hive配置"""
        return {
            'enable_hive_support': self.config.getboolean('hive', 'enable_hive_support', fallback=False),
            'exec_mysql_database': self.config.get('hive', 'exec_mysql_database', fallback='gmall_hive'),
            'metastore_database': self.config.get('hive', 'metastore_database', fallback='hive'),
            'metastore_username': self.config.get('hive', 'metastore_username', fallback='hive'),
            'metastore_password': self.config.get('hive', 'metastore_password', fallback='hive')
        }
    
    def get_paths_config(self):
        """获取路径配置"""
        return {
            'ods_path': self.config.get('paths', 'ods_path'),
            'dwd_path': self.config.get('paths', 'dwd_path'),
            'dim_path': self.config.get('paths', 'dim_path'),
            'dws_path': self.config.get('paths', 'dws_path'),
            'ads_path': self.config.get('paths', 'ads_path')
        }

    def get_runtime_config(self):
        """获取运行期控制配置，用于控制退出行为等"""
        try:
            section = 'runtime'
            # 如果没有该节，提供安全的默认值
            stop_spark_on_exit = self.config.getboolean(section, 'stop_spark_on_exit', fallback=True)
            wait_on_exit = self.config.getboolean(section, 'wait_on_exit', fallback=False)
            wait_seconds = self.config.getint(section, 'wait_seconds', fallback=0)
        except Exception:
            stop_spark_on_exit, wait_on_exit, wait_seconds = True, False, 0

        return {
            'stop_spark_on_exit': stop_spark_on_exit,
            'wait_on_exit': wait_on_exit,
            'wait_seconds': wait_seconds
        }
    
    def is_env_config_enabled(self):
        """检查是否启用环境变量配置"""
        try:
            return self.config.getboolean('environment', 'enable_env_config', fallback=True)
        except:
            return True  # 默认启用
    
    def get_environment_config(self):
        """获取环境变量配置"""
        return {
            'enable_env_config': self.is_env_config_enabled(),
            'hadoop_home': self.config.get('environment', 'hadoop_home', fallback=''),
            'hive_home': self.config.get('environment', 'hive_home', fallback=''),
            'java_home': self.config.get('environment', 'java_home', fallback=''),
            'pyspark_python': self.config.get('environment', 'pyspark_python', fallback=''),
            'pyspark_driver_python': self.config.get('environment', 'pyspark_driver_python', fallback='')
        }
    def get_mysql_url(self, database=None):
        """获取MySQL连接URL"""
        mysql_config = self.get_mysql_config()
        db_name = database if database else mysql_config['database']
        return f"jdbc:mysql://{mysql_config['host']}:{mysql_config['port']}/{db_name}?useSSL=false&useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai"
    
    def get_hive_mysql_url(self):
        """获取Hive专用MySQL连接URL（用于ADS结果输出）"""
        hive_config = self.get_hive_config()
        return self.get_mysql_url(hive_config['exec_mysql_database'])
    
    def get_hive_metastore_url(self):
        """获取Hive元数据存储MySQL连接URL"""
        hive_config = self.get_hive_config()
        return self.get_mysql_url(hive_config['metastore_database'])