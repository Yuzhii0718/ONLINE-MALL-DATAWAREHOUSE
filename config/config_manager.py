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
    
    def get_paths_config(self):
        """获取路径配置"""
        return {
            'ods_path': self.config.get('paths', 'ods_path'),
            'dwd_path': self.config.get('paths', 'dwd_path'),
            'dim_path': self.config.get('paths', 'dim_path'),
            'dws_path': self.config.get('paths', 'dws_path'),
            'ads_path': self.config.get('paths', 'ads_path')
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
            'java_home': self.config.get('environment', 'java_home', fallback=''),
            'pyspark_python': self.config.get('environment', 'pyspark_python', fallback=''),
            'pyspark_driver_python': self.config.get('environment', 'pyspark_driver_python', fallback='')
        }
    
    def get_mysql_url(self):
        """获取MySQL连接URL"""
        mysql_config = self.get_mysql_config()
        return f"jdbc:mysql://{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}?useSSL=false&useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai"