"""
Hive ADS - 用户相关指标
包含：
- 用户统计
"""
import logging
from config.config_manager import ConfigManager


class HiveADSUsers:
    def __init__(self, spark):
        self.spark = spark
        self.config_manager = ConfigManager()
        self.mysql_config = self.config_manager.get_mysql_config()
        self.hive_mysql_url = self.config_manager.get_hive_mysql_url()
    
    def build_ads_user_stats(self):
        """构建用户统计"""
        try:
            print("\n开始构建用户统计...")
            
            sql = """
            SELECT 
                COUNT(DISTINCT user_id) as total_user_count,
                COUNT(DISTINCT CASE WHEN dt = date_format(current_timestamp(), 'yyyy-MM-dd') 
                      THEN user_id END) as new_user_count
            FROM dwd_user_register
            """
            
            result_df = self.spark.sql(sql)
            
            # 写入MySQL
            result_df.write \
                .format("jdbc") \
                .option("url", self.hive_mysql_url) \
                .option("dbtable", "ads_user_stats") \
                .option("user", self.mysql_config['username']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", self.mysql_config['driver']) \
                .mode("overwrite") \
                .save()
            
            print(f"✓ 用户统计构建完成")
            return True
            
        except Exception as e:
            print(f"✗ 用户统计构建失败: {e}")
            logging.error(f"构建ads_user_stats失败: {e}", exc_info=True)
            return False
