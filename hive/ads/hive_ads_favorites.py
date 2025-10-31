"""
Hive ADS - 收藏相关指标
包含：
- 商品收藏Top10
"""
import logging
from config.config_manager import ConfigManager


class HiveADSFavorites:
    def __init__(self, spark):
        self.spark = spark
        self.config_manager = ConfigManager()
        self.mysql_config = self.config_manager.get_mysql_config()
        self.hive_mysql_url = self.config_manager.get_hive_mysql_url()
    
    def build_ads_sku_favor_count_top10(self):
        """构建商品收藏Top10"""
        try:
            print("\n开始构建商品收藏Top10...")
            
            sql = """
            SELECT 
                dif.sku_id,
                ds.sku_name,
                COUNT(*) as favor_count
            FROM dwd_interaction_favor dif
            LEFT JOIN dim_sku ds ON dif.sku_id = ds.sku_id
            GROUP BY dif.sku_id, ds.sku_name
            ORDER BY favor_count DESC
            LIMIT 10
            """
            
            result_df = self.spark.sql(sql)
            
            # 写入MySQL
            result_df.write \
                .format("jdbc") \
                .option("url", self.hive_mysql_url) \
                .option("dbtable", "ads_sku_favor_count_top10") \
                .option("user", self.mysql_config['username']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", self.mysql_config['driver']) \
                .mode("overwrite") \
                .save()
            
            count = result_df.count()
            print(f"✓ 商品收藏Top10构建完成，记录数: {count}")
            return True
            
        except Exception as e:
            print(f"✗ 商品收藏Top10构建失败: {e}")
            logging.error(f"构建ads_sku_favor_count_top10失败: {e}", exc_info=True)
            return False
