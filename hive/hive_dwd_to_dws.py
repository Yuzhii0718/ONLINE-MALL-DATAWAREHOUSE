"""
Hive模式: DWD到DWS层轻度聚合
使用HiveQL进行轻度聚合
"""
import logging
from tools.spark_util import SparkUtil
from config.config_manager import ConfigManager

class HiveDWDToDWS:
    def __init__(self):
        self.spark = SparkUtil.create_spark_session("Hive-DWD-To-DWS")
        self.config_manager = ConfigManager()
        self.paths_config = self.config_manager.get_paths_config()
    
    def build_dws_trade_user_order_td(self):
        """构建交易域用户粒度订单汇总表"""
        try:
            print("\n开始构建交易域用户粒度订单汇总表...")
            
            select_sql = """
            SELECT 
                user_id,
                COUNT(DISTINCT order_id) as order_count,
                SUM(split_total_amount) as order_total_amount,
                date_format(current_timestamp(), 'yyyy-MM-dd') as dt
            FROM dwd_trade_order_detail
            GROUP BY user_id
            """
            
            dws_df = self.spark.sql(select_sql)
            target_path = f"{self.paths_config['dws_path']}/dws_trade_user_order_td"
            
            self.spark.sql("DROP TABLE IF EXISTS dws_trade_user_order_td")
            dws_df.write \
                .mode("overwrite") \
                .format("parquet") \
                .option("path", target_path) \
                .saveAsTable("dws_trade_user_order_td")
            
            count = self.spark.table("dws_trade_user_order_td").count()
            print(f"✓ 交易域用户粒度订单汇总表构建完成，记录数: {count}")
            return True
            
        except Exception as e:
            print(f"✗ 交易域用户粒度订单汇总表构建失败: {e}")
            logging.error(f"构建dws_trade_user_order_td失败: {e}", exc_info=True)
            return False
    
    def build_dws_trade_sku_order_td(self):
        """构建交易域商品粒度订单汇总表"""
        try:
            print("\n开始构建交易域商品粒度订单汇总表...")
            
            select_sql = """
            SELECT 
                sku_id,
                sku_name,
                COUNT(DISTINCT order_id) as order_count,
                SUM(sku_num) as order_num,
                SUM(split_total_amount) as order_amount,
                date_format(current_timestamp(), 'yyyy-MM-dd') as dt
            FROM dwd_trade_order_detail
            GROUP BY sku_id, sku_name
            """
            
            dws_df = self.spark.sql(select_sql)
            target_path = f"{self.paths_config['dws_path']}/dws_trade_sku_order_td"
            
            self.spark.sql("DROP TABLE IF EXISTS dws_trade_sku_order_td")
            dws_df.write \
                .mode("overwrite") \
                .format("parquet") \
                .option("path", target_path) \
                .saveAsTable("dws_trade_sku_order_td")
            
            count = self.spark.table("dws_trade_sku_order_td").count()
            print(f"✓ 交易域商品粒度订单汇总表构建完成，记录数: {count}")
            return True
            
        except Exception as e:
            print(f"✗ 交易域商品粒度订单汇总表构建失败: {e}")
            logging.error(f"构建dws_trade_sku_order_td失败: {e}", exc_info=True)
            return False
    
    def build_dws_trade_province_order_td(self):
        """构建交易域地区粒度订单汇总表"""
        try:
            print("\n开始构建交易域地区粒度订单汇总表...")
            
            select_sql = """
            SELECT 
                province_id,
                COUNT(DISTINCT order_id) as order_count,
                SUM(split_total_amount) as order_amount,
                date_format(current_timestamp(), 'yyyy-MM-dd') as dt
            FROM dwd_trade_order_detail
            GROUP BY province_id
            """
            
            dws_df = self.spark.sql(select_sql)
            target_path = f"{self.paths_config['dws_path']}/dws_trade_province_order_td"
            
            self.spark.sql("DROP TABLE IF EXISTS dws_trade_province_order_td")
            dws_df.write \
                .mode("overwrite") \
                .format("parquet") \
                .option("path", target_path) \
                .saveAsTable("dws_trade_province_order_td")
            
            count = self.spark.table("dws_trade_province_order_td").count()
            print(f"✓ 交易域地区粒度订单汇总表构建完成，记录数: {count}")
            return True
            
        except Exception as e:
            print(f"✗ 交易域地区粒度订单汇总表构建失败: {e}")
            logging.error(f"构建dws_trade_province_order_td失败: {e}", exc_info=True)
            return False
    
    def build_dws_user_action_td(self):
        """构建用户行为汇总表"""
        try:
            print("\n开始构建用户行为汇总表...")
            
            select_sql = """
            SELECT 
                user_id,
                COUNT(DISTINCT favor_id) as favor_count,
                date_format(current_timestamp(), 'yyyy-MM-dd') as dt
            FROM dwd_interaction_favor
            GROUP BY user_id
            """
            
            dws_df = self.spark.sql(select_sql)
            target_path = f"{self.paths_config['dws_path']}/dws_user_action_td"
            
            self.spark.sql("DROP TABLE IF EXISTS dws_user_action_td")
            dws_df.write \
                .mode("overwrite") \
                .format("parquet") \
                .option("path", target_path) \
                .saveAsTable("dws_user_action_td")
            
            count = self.spark.table("dws_user_action_td").count()
            print(f"✓ 用户行为汇总表构建完成，记录数: {count}")
            return True
            
        except Exception as e:
            print(f"✗ 用户行为汇总表构建失败: {e}")
            logging.error(f"构建dws_user_action_td失败: {e}", exc_info=True)
            return False
    
    def build_all_dws(self):
        """构建所有DWS汇总表"""
        print("=== 开始Hive DWD到DWS层构建 ===")
        
        dws_tables = [
            ("交易域用户粒度订单汇总", self.build_dws_trade_user_order_td),
            ("交易域商品粒度订单汇总", self.build_dws_trade_sku_order_td),
            ("交易域地区粒度订单汇总", self.build_dws_trade_province_order_td),
            ("用户行为汇总", self.build_dws_user_action_td)
        ]
        
        success_count = 0
        for dws_name, build_func in dws_tables:
            if build_func():
                success_count += 1
        
        print(f"\n=== DWS层构建完成 ===")
        print(f"成功: {success_count}/{len(dws_tables)}")
        
        return success_count == len(dws_tables)
