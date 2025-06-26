"""
DWD/DIM到DWS层轻度聚合
从DWD事实表和DIM维度表构建轻度聚合表
"""
import logging
from tools.spark_util import SparkUtil
from config.config_manager import ConfigManager
from pyspark.sql.functions import *
from pyspark.sql.types import *

class DWDToDWS:
    def __init__(self):
        self.spark = SparkUtil.get_spark_session("DWD-To-DWS")
        self.config_manager = ConfigManager()
        self.paths_config = self.config_manager.get_paths_config()
    
    def build_dws_trade_user_sku_order_nd(self):
        """构建交易域用户商品粒度订单汇总表"""
        try:
            print("\n开始构建交易域用户商品粒度订单汇总表...")
            
            # 读取交易域下单事务事实表
            dwd_trade_order_detail_path = f"{self.paths_config['dwd_path']}/dwd_trade_order_detail"
            dwd_trade_order_detail_df = SparkUtil.read_from_hdfs(dwd_trade_order_detail_path, spark=self.spark)
            
            # 按用户和商品聚合订单数据
            dws_trade_user_sku_order_nd = dwd_trade_order_detail_df \
                .groupBy("user_id", "sku_id") \
                .agg(
                    countDistinct("order_id").alias("order_count"),
                    sum("sku_num").alias("order_num"),
                    sum("split_total_amount").alias("order_amount"),
                    sum("split_activity_amount").alias("activity_reduce_amount"),
                    sum("split_coupon_amount").alias("coupon_reduce_amount")
                )
            
            # 写入DWS层
            dws_path = f"{self.paths_config['dws_path']}/dws_trade_user_sku_order_nd"
            SparkUtil.write_to_hdfs(dws_trade_user_sku_order_nd, dws_path, spark=self.spark)
            
            print(f"✓ 交易域用户商品粒度订单汇总表构建完成，记录数: {dws_trade_user_sku_order_nd.count()}")
            
        except Exception as e:
            print(f"✗ 交易域用户商品粒度订单汇总表构建失败: {e}")
            raise
    
    def build_dws_trade_user_order_nd(self):
        """构建交易域用户粒度订单汇总表"""
        try:
            print("\n开始构建交易域用户粒度订单汇总表...")
            
            # 读取交易域下单事务事实表
            dwd_trade_order_detail_path = f"{self.paths_config['dwd_path']}/dwd_trade_order_detail"
            dwd_trade_order_detail_df = SparkUtil.read_from_hdfs(dwd_trade_order_detail_path, spark=self.spark)
            
            # 按用户聚合订单数据
            dws_trade_user_order_nd = dwd_trade_order_detail_df \
                .groupBy("user_id") \
                .agg(
                    countDistinct("order_id").alias("order_count"),
                    sum("sku_num").alias("order_num"),
                    sum("split_total_amount").alias("order_amount"),
                    sum("split_activity_amount").alias("activity_reduce_amount"),
                    sum("split_coupon_amount").alias("coupon_reduce_amount")
                )
            
            # 写入DWS层
            dws_path = f"{self.paths_config['dws_path']}/dws_trade_user_order_nd"
            SparkUtil.write_to_hdfs(dws_trade_user_order_nd, dws_path, spark=self.spark)
            
            print(f"✓ 交易域用户粒度订单汇总表构建完成，记录数: {dws_trade_user_order_nd.count()}")
            
        except Exception as e:
            print(f"✗ 交易域用户粒度订单汇总表构建失败: {e}")
            raise
    
    def build_dws_trade_sku_order_nd(self):
        """构建交易域商品粒度订单汇总表"""
        try:
            print("\n开始构建交易域商品粒度订单汇总表...")
            
            # 读取交易域下单事务事实表
            dwd_trade_order_detail_path = f"{self.paths_config['dwd_path']}/dwd_trade_order_detail"
            dwd_trade_order_detail_df = SparkUtil.read_from_hdfs(dwd_trade_order_detail_path, spark=self.spark)
            
            # 按商品聚合订单数据
            dws_trade_sku_order_nd = dwd_trade_order_detail_df \
                .groupBy("sku_id") \
                .agg(
                    countDistinct("order_id").alias("order_count"),
                    countDistinct("user_id").alias("user_count"),
                    sum("sku_num").alias("order_num"),
                    sum("split_total_amount").alias("order_amount"),
                    sum("split_activity_amount").alias("activity_reduce_amount"),
                    sum("split_coupon_amount").alias("coupon_reduce_amount")
                )
            
            # 写入DWS层
            dws_path = f"{self.paths_config['dws_path']}/dws_trade_sku_order_nd"
            SparkUtil.write_to_hdfs(dws_trade_sku_order_nd, dws_path, spark=self.spark)
            
            print(f"✓ 交易域商品粒度订单汇总表构建完成，记录数: {dws_trade_sku_order_nd.count()}")
            
        except Exception as e:
            print(f"✗ 交易域商品粒度订单汇总表构建失败: {e}")
            raise
    
    def build_dws_trade_province_order_nd(self):
        """构建交易域地区粒度订单汇总表"""
        try:
            print("\n开始构建交易域地区粒度订单汇总表...")
            
            # 读取交易域下单事务事实表
            dwd_trade_order_detail_path = f"{self.paths_config['dwd_path']}/dwd_trade_order_detail"
            dwd_trade_order_detail_df = SparkUtil.read_from_hdfs(dwd_trade_order_detail_path, spark=self.spark)
            
            # 按地区聚合订单数据
            dws_trade_province_order_nd = dwd_trade_order_detail_df \
                .groupBy("province_id") \
                .agg(
                    countDistinct("order_id").alias("order_count"),
                    countDistinct("user_id").alias("user_count"),
                    sum("sku_num").alias("order_num"),
                    sum("split_total_amount").alias("order_amount"),
                    sum("split_activity_amount").alias("activity_reduce_amount"),
                    sum("split_coupon_amount").alias("coupon_reduce_amount")
                )
            
            # 写入DWS层
            dws_path = f"{self.paths_config['dws_path']}/dws_trade_province_order_nd"
            SparkUtil.write_to_hdfs(dws_trade_province_order_nd, dws_path, spark=self.spark)
            
            print(f"✓ 交易域地区粒度订单汇总表构建完成，记录数: {dws_trade_province_order_nd.count()}")
            
        except Exception as e:
            print(f"✗ 交易域地区粒度订单汇总表构建失败: {e}")
            raise
    
    def build_dws_user_user_login_nd(self):
        """构建用户域用户粒度登录汇总表"""
        try:
            print("\n开始构建用户域用户粒度登录汇总表...")
            
            # 读取用户域用户注册事务事实表
            dwd_user_register_path = f"{self.paths_config['dwd_path']}/dwd_user_register"
            dwd_user_register_df = SparkUtil.read_from_hdfs(dwd_user_register_path, spark=self.spark)
            
            # 按用户聚合登录数据（这里用注册数据代替）
            dws_user_user_login_nd = dwd_user_register_df \
                .groupBy("user_id") \
                .agg(
                    count("*").alias("login_count"),
                    max("create_time").alias("last_login_time")
                )
            
            # 写入DWS层
            dws_path = f"{self.paths_config['dws_path']}/dws_user_user_login_nd"
            SparkUtil.write_to_hdfs(dws_user_user_login_nd, dws_path, spark=self.spark)
            
            print(f"✓ 用户域用户粒度登录汇总表构建完成，记录数: {dws_user_user_login_nd.count()}")
            
        except Exception as e:
            print(f"✗ 用户域用户粒度登录汇总表构建失败: {e}")
            raise
    
    def build_dws_tool_user_coupon_coupon_used_nd(self):
        """构建工具域用户优惠券粒度优惠券使用汇总表"""
        try:
            print("\n开始构建工具域用户优惠券粒度优惠券使用汇总表...")
            
            # 读取工具域优惠券使用事务事实表
            dwd_tool_coupon_used_path = f"{self.paths_config['dwd_path']}/dwd_tool_coupon_used"
            dwd_tool_coupon_used_df = SparkUtil.read_from_hdfs(dwd_tool_coupon_used_path, spark=self.spark)
            
            # 按用户和优惠券聚合使用数据
            dws_tool_user_coupon_coupon_used_nd = dwd_tool_coupon_used_df \
                .groupBy("user_id", "coupon_id") \
                .agg(
                    count("*").alias("used_count"),
                    max("used_time").alias("last_used_time")
                )
            
            # 写入DWS层
            dws_path = f"{self.paths_config['dws_path']}/dws_tool_user_coupon_coupon_used_nd"
            SparkUtil.write_to_hdfs(dws_tool_user_coupon_coupon_used_nd, dws_path, spark=self.spark)
            
            print(f"✓ 工具域用户优惠券粒度优惠券使用汇总表构建完成，记录数: {dws_tool_user_coupon_coupon_used_nd.count()}")
            
        except Exception as e:
            print(f"✗ 工具域用户优惠券粒度优惠券使用汇总表构建失败: {e}")
            raise
    
    def build_dws_interaction_sku_favor_add_nd(self):
        """构建互动域商品粒度收藏汇总表"""
        try:
            print("\n开始构建互动域商品粒度收藏汇总表...")
            
            # 读取互动域收藏商品事务事实表
            dwd_interaction_favor_add_path = f"{self.paths_config['dwd_path']}/dwd_interaction_favor_add"
            dwd_interaction_favor_add_df = SparkUtil.read_from_hdfs(dwd_interaction_favor_add_path, spark=self.spark)
            
            # 按商品聚合收藏数据
            dws_interaction_sku_favor_add_nd = dwd_interaction_favor_add_df \
                .groupBy("sku_id") \
                .agg(
                    count("*").alias("favor_count"),
                    countDistinct("user_id").alias("favor_user_count")
                )
            
            # 写入DWS层
            dws_path = f"{self.paths_config['dws_path']}/dws_interaction_sku_favor_add_nd"
            SparkUtil.write_to_hdfs(dws_interaction_sku_favor_add_nd, dws_path, spark=self.spark)
            
            print(f"✓ 互动域商品粒度收藏汇总表构建完成，记录数: {dws_interaction_sku_favor_add_nd.count()}")
            
        except Exception as e:
            print(f"✗ 互动域商品粒度收藏汇总表构建失败: {e}")
            raise
    
    def build_all_dws(self):
        """构建所有DWS汇总表"""
        print("=== 开始构建DWS汇总表 ===")
        
        dws_list = [
            ("交易域用户商品粒度订单汇总表", self.build_dws_trade_user_sku_order_nd),
            ("交易域用户粒度订单汇总表", self.build_dws_trade_user_order_nd),
            ("交易域商品粒度订单汇总表", self.build_dws_trade_sku_order_nd),
            ("交易域地区粒度订单汇总表", self.build_dws_trade_province_order_nd),
            ("用户域用户粒度登录汇总表", self.build_dws_user_user_login_nd),
            ("工具域用户优惠券粒度优惠券使用汇总表", self.build_dws_tool_user_coupon_coupon_used_nd),
            ("互动域商品粒度收藏汇总表", self.build_dws_interaction_sku_favor_add_nd)
        ]
        
        success_count = 0
        failure_count = 0
        
        for dws_name, dws_func in dws_list:
            try:
                dws_func()
                success_count += 1
            except Exception as e:
                failure_count += 1
                print(f"✗ {dws_name} 构建失败: {e}")
                continue
        
        print(f"\n=== DWS层构建完成 ===")
        print(f"成功: {success_count}张汇总表")
        print(f"失败: {failure_count}张汇总表")
        print(f"总计: {len(dws_list)}张汇总表")

def main():
    """主函数"""
    try:
        dwd_to_dws = DWDToDWS()
        dwd_to_dws.build_all_dws()
        
    except Exception as e:
        logging.error(f"DWD到DWS构建失败: {e}")
        print(f"✗ DWD到DWS构建失败: {e}")
    finally:
        SparkUtil.stop_spark_session()

if __name__ == "__main__":
    # 设置日志
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    main()
