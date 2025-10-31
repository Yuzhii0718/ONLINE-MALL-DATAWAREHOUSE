"""
Hive模式: ODS到DWD层事实表构建
使用HiveQL从ODS层构建各事务事实表
"""
import logging
from tools.spark_util import SparkUtil
from config.config_manager import ConfigManager

class HiveODSToDWD:
    def __init__(self):
        self.spark = SparkUtil.create_spark_session("Hive-ODS-To-DWD")
        self.config_manager = ConfigManager()
        self.paths_config = self.config_manager.get_paths_config()
    
    def build_dwd_trade_order_detail(self):
        """构建交易域下单事务事实表"""
        try:
            print("\n开始构建交易域下单事务事实表...")
            
            select_sql = """
            SELECT 
                od.id as order_detail_id,
                od.order_id,
                oi.user_id,
                od.sku_id,
                od.sku_name,
                od.order_price,
                od.sku_num,
                od.split_total_amount,
                od.split_activity_amount,
                od.split_coupon_amount,
                oda.activity_id,
                oda.activity_rule_id,
                odc.coupon_id,
                odc.coupon_use_id,
                oi.province_id,
                oi.order_status,
                oi.payment_way,
                oi.total_amount,
                oi.activity_reduce_amount,
                oi.coupon_reduce_amount,
                oi.original_total_amount,
                oi.feight_fee,
                oi.create_time,
                oi.operate_time,
                date_format(oi.create_time, 'yyyy-MM-dd') as dt
            FROM ods_order_detail od
            INNER JOIN ods_order_info oi ON od.order_id = oi.id
            LEFT JOIN ods_order_detail_activity oda ON od.id = oda.order_detail_id
            LEFT JOIN ods_order_detail_coupon odc ON od.id = odc.order_detail_id
            """
            
            dwd_df = self.spark.sql(select_sql)
            target_path = f"{self.paths_config['dwd_path']}/dwd_trade_order_detail"
            
            self.spark.sql("DROP TABLE IF EXISTS dwd_trade_order_detail")
            dwd_df.write \
                .mode("overwrite") \
                .format("parquet") \
                .option("path", target_path) \
                .saveAsTable("dwd_trade_order_detail")
            
            count = self.spark.table("dwd_trade_order_detail").count()
            print(f"✓ 交易域下单事务事实表构建完成，记录数: {count}")
            return True
            
        except Exception as e:
            print(f"✗ 交易域下单事务事实表构建失败: {e}")
            logging.error(f"构建dwd_trade_order_detail失败: {e}", exc_info=True)
            return False
    
    def build_dwd_trade_pay_detail(self):
        """构建交易域支付成功事务事实表"""
        try:
            print("\n开始构建交易域支付成功事务事实表...")
            
            select_sql = """
            SELECT 
                od.id as order_detail_id,
                od.order_id,
                oi.user_id,
                od.sku_id,
                od.sku_name,
                od.order_price,
                od.sku_num,
                pi.payment_type,
                pi.callback_time as payment_time,
                od.split_total_amount as payment_amount,
                oi.province_id,
                date_format(pi.callback_time, 'yyyy-MM-dd') as dt
            FROM ods_payment_info pi
            INNER JOIN ods_order_info oi ON pi.order_id = oi.id
            INNER JOIN ods_order_detail od ON oi.id = od.order_id
            WHERE pi.payment_status = '1602'
            """
            
            dwd_df = self.spark.sql(select_sql)
            target_path = f"{self.paths_config['dwd_path']}/dwd_trade_pay_detail"
            
            self.spark.sql("DROP TABLE IF EXISTS dwd_trade_pay_detail")
            dwd_df.write \
                .mode("overwrite") \
                .format("parquet") \
                .option("path", target_path) \
                .saveAsTable("dwd_trade_pay_detail")
            
            count = self.spark.table("dwd_trade_pay_detail").count()
            print(f"✓ 交易域支付成功事务事实表构建完成，记录数: {count}")
            return True
            
        except Exception as e:
            print(f"✗ 交易域支付成功事务事实表构建失败: {e}")
            logging.error(f"构建dwd_trade_pay_detail失败: {e}", exc_info=True)
            return False
    
    def build_dwd_user_register(self):
        """构建用户域用户注册事务事实表"""
        try:
            print("\n开始构建用户域用户注册事务事实表...")
            
            select_sql = """
            SELECT 
                id as user_id,
                create_time,
                date_format(create_time, 'yyyy-MM-dd') as dt
            FROM ods_user_info
            WHERE create_time IS NOT NULL
            """
            
            dwd_df = self.spark.sql(select_sql)
            target_path = f"{self.paths_config['dwd_path']}/dwd_user_register"
            
            self.spark.sql("DROP TABLE IF EXISTS dwd_user_register")
            dwd_df.write \
                .mode("overwrite") \
                .format("parquet") \
                .option("path", target_path) \
                .saveAsTable("dwd_user_register")
            
            count = self.spark.table("dwd_user_register").count()
            print(f"✓ 用户域用户注册事务事实表构建完成，记录数: {count}")
            return True
            
        except Exception as e:
            print(f"✗ 用户域用户注册事务事实表构建失败: {e}")
            logging.error(f"构建dwd_user_register失败: {e}", exc_info=True)
            return False
    
    def build_dwd_interaction_favor(self):
        """构建互动域收藏事务事实表"""
        try:
            print("\n开始构建互动域收藏事务事实表...")
            
            select_sql = """
            SELECT 
                id as favor_id,
                user_id,
                sku_id,
                create_time,
                date_format(create_time, 'yyyy-MM-dd') as dt
            FROM ods_favor_info
            WHERE is_cancel = '0'
            """
            
            dwd_df = self.spark.sql(select_sql)
            target_path = f"{self.paths_config['dwd_path']}/dwd_interaction_favor"
            
            self.spark.sql("DROP TABLE IF EXISTS dwd_interaction_favor")
            dwd_df.write \
                .mode("overwrite") \
                .format("parquet") \
                .option("path", target_path) \
                .saveAsTable("dwd_interaction_favor")
            
            count = self.spark.table("dwd_interaction_favor").count()
            print(f"✓ 互动域收藏事务事实表构建完成，记录数: {count}")
            return True
            
        except Exception as e:
            print(f"✗ 互动域收藏事务事实表构建失败: {e}")
            logging.error(f"构建dwd_interaction_favor失败: {e}", exc_info=True)
            return False
    
    def build_all_dwds(self):
        """构建所有DWD事实表"""
        print("=== 开始Hive ODS到DWD层构建 ===")
        
        dwds = [
            ("交易域下单事务", self.build_dwd_trade_order_detail),
            ("交易域支付事务", self.build_dwd_trade_pay_detail),
            ("用户域注册事务", self.build_dwd_user_register),
            ("互动域收藏事务", self.build_dwd_interaction_favor)
        ]
        
        success_count = 0
        for dwd_name, build_func in dwds:
            if build_func():
                success_count += 1
        
        print(f"\n=== DWD层构建完成 ===")
        print(f"成功: {success_count}/{len(dwds)}")
        
        return success_count == len(dwds)
