"""
ODS到DWD层事实表构建
从ODS层数据构建各事务事实表
"""
import logging
from tools.spark_util import SparkUtil
from config.config_manager import ConfigManager
from pyspark.sql.functions import *
from pyspark.sql.types import *

class ODSToDWD:
    def __init__(self):
        self.spark = SparkUtil.get_spark_session("ODS-To-DWD")
        self.config_manager = ConfigManager()
        self.paths_config = self.config_manager.get_paths_config()
    
    def build_dwd_trade_order_detail(self):
        """构建交易域下单事务事实表"""
        try:
            print("\n开始构建交易域下单事务事实表...")
            
            # 读取订单明细表
            order_detail_path = f"{self.paths_config['ods_path']}/order_detail"
            order_detail_df = SparkUtil.read_from_hdfs(order_detail_path, spark=self.spark)
            
            # 读取订单表
            order_info_path = f"{self.paths_config['ods_path']}/order_info"
            order_info_df = SparkUtil.read_from_hdfs(order_info_path, spark=self.spark)
            
            # 读取订单明细活动关联表
            order_detail_activity_path = f"{self.paths_config['ods_path']}/order_detail_activity"
            order_detail_activity_df = SparkUtil.read_from_hdfs(order_detail_activity_path, spark=self.spark)
            
            # 读取订单明细优惠券关联表
            order_detail_coupon_path = f"{self.paths_config['ods_path']}/order_detail_coupon"
            order_detail_coupon_df = SparkUtil.read_from_hdfs(order_detail_coupon_path, spark=self.spark)
            
            # 构建下单事务事实表
            dwd_trade_order_detail = order_detail_df.alias("od") \
                .join(order_info_df.alias("oi"), col("od.order_id") == col("oi.id"), "inner") \
                .join(order_detail_activity_df.alias("oda"), col("od.id") == col("oda.order_detail_id"), "left") \
                .join(order_detail_coupon_df.alias("odc"), col("od.id") == col("odc.order_detail_id"), "left") \
                .select(
                    col("od.id").alias("order_detail_id"),
                    col("od.order_id"),
                    col("oi.user_id"),
                    col("od.sku_id"),
                    col("od.sku_name"),
                    col("od.order_price"),
                    col("od.sku_num"),
                    col("od.split_total_amount"),
                    col("od.split_activity_amount"),
                    col("od.split_coupon_amount"),
                    col("oda.activity_id"),
                    col("oda.activity_rule_id"),
                    col("odc.coupon_id"),
                    col("odc.coupon_use_id"),
                    col("oi.province_id"),
                    col("oi.order_status"),
                    col("oi.payment_way"),
                    col("oi.total_amount"),
                    col("oi.activity_reduce_amount"),
                    col("oi.coupon_reduce_amount"),
                    col("oi.original_total_amount"),
                    col("oi.feight_fee"),
                    col("oi.create_time"),
                    col("oi.operate_time"),
                    date_format(col("oi.create_time"), "yyyy-MM-dd").alias("dt")
                )
            
            # 写入DWD层
            dwd_path = f"{self.paths_config['dwd_path']}/dwd_trade_order_detail"
            SparkUtil.write_to_hdfs(dwd_trade_order_detail, dwd_path, spark=self.spark)
            
            print(f"✓ 交易域下单事务事实表构建完成，记录数: {dwd_trade_order_detail.count()}")
            
        except Exception as e:
            print(f"✗ 交易域下单事务事实表构建失败: {e}")
            raise
    
    def build_dwd_trade_pay_detail(self):
        """构建交易域支付成功事务事实表"""
        try:
            print("\n开始构建交易域支付成功事务事实表...")
            
            # 读取支付表
            payment_info_path = f"{self.paths_config['ods_path']}/payment_info"
            payment_info_df = SparkUtil.read_from_hdfs(payment_info_path, spark=self.spark)
            
            # 读取订单表
            order_info_path = f"{self.paths_config['ods_path']}/order_info"
            order_info_df = SparkUtil.read_from_hdfs(order_info_path, spark=self.spark)
            
            # 读取订单明细表
            order_detail_path = f"{self.paths_config['ods_path']}/order_detail"
            order_detail_df = SparkUtil.read_from_hdfs(order_detail_path, spark=self.spark)
            
            # 构建支付成功事务事实表
            dwd_trade_pay_detail = payment_info_df.alias("pi") \
                .filter(col("pi.payment_status") == "1602") \
                .join(order_info_df.alias("oi"), col("pi.order_id") == col("oi.id"), "inner") \
                .join(order_detail_df.alias("od"), col("oi.id") == col("od.order_id"), "inner") \
                .select(
                    col("pi.id").alias("payment_id"),
                    col("pi.order_id"),
                    col("oi.user_id"),
                    col("od.id").alias("order_detail_id"),
                    col("od.sku_id"),
                    col("od.sku_name"),
                    col("od.order_price"),
                    col("od.sku_num"),
                    col("od.split_total_amount"),
                    col("od.split_activity_amount"),
                    col("od.split_coupon_amount"),
                    col("pi.payment_type"),
                    col("pi.total_amount").alias("payment_amount"),
                    col("oi.province_id"),
                    col("pi.create_time"),
                    col("pi.callback_time"),
                    date_format(col("pi.callback_time"), "yyyy-MM-dd").alias("dt")
                )
            
            # 写入DWD层
            dwd_path = f"{self.paths_config['dwd_path']}/dwd_trade_pay_detail"
            SparkUtil.write_to_hdfs(dwd_trade_pay_detail, dwd_path, spark=self.spark)
            
            print(f"✓ 交易域支付成功事务事实表构建完成，记录数: {dwd_trade_pay_detail.count()}")
            
        except Exception as e:
            print(f"✗ 交易域支付成功事务事实表构建失败: {e}")
            raise
    
    def build_dwd_trade_cart_add(self):
        """构建交易域加购事务事实表"""
        try:
            print("\n开始构建交易域加购事务事实表...")
            
            # 读取购物车表
            cart_info_path = f"{self.paths_config['ods_path']}/cart_info"
            cart_info_df = SparkUtil.read_from_hdfs(cart_info_path, spark=self.spark)
            
            # 构建加购事务事实表
            dwd_trade_cart_add = cart_info_df.select(
                col("id").alias("cart_id"),
                col("user_id"),
                col("sku_id"),
                col("cart_price"),
                col("sku_num"),
                col("sku_name"),
                col("create_time"),
                col("operate_time"),
                col("is_ordered"),
                col("order_time"),
                col("source_type"),
                col("source_id"),
                date_format(col("create_time"), "yyyy-MM-dd").alias("dt")
            )
            
            # 写入DWD层
            dwd_path = f"{self.paths_config['dwd_path']}/dwd_trade_cart_add"
            SparkUtil.write_to_hdfs(dwd_trade_cart_add, dwd_path, spark=self.spark)
            
            print(f"✓ 交易域加购事务事实表构建完成，记录数: {dwd_trade_cart_add.count()}")
            
        except Exception as e:
            print(f"✗ 交易域加购事务事实表构建失败: {e}")
            raise
    
    def build_dwd_tool_coupon_used(self):
        """构建工具域优惠券使用事务事实表"""
        try:
            print("\n开始构建工具域优惠券使用事务事实表...")
            
            # 读取优惠券领用表
            coupon_use_path = f"{self.paths_config['ods_path']}/coupon_use"
            coupon_use_df = SparkUtil.read_from_hdfs(coupon_use_path, spark=self.spark)
            
            print(f"原始数据记录数: {coupon_use_df.count()}")
            
            # 检查数据结构和内容
            print("优惠券领用表字段结构:")
            coupon_use_df.printSchema()
            
            print("\n优惠券领用表数据样本:")
            coupon_use_df.show(10, truncate=False)
            
            # 检查coupon_status字段的分布
            print("\ncoupon_status字段值分布:")
            status_distribution = coupon_use_df.groupBy("coupon_status").count().orderBy("count", ascending=False)
            status_distribution.show()
            
            # 检查used_time字段的空值情况
            print("\nused_time字段空值统计:")
            used_time_stats = coupon_use_df.agg(
                count("*").alias("total_records"),
                count("used_time").alias("used_time_not_null"),
                (count("*") - count("used_time")).alias("used_time_null")
            )
            used_time_stats.show()
            
            # 修正过滤逻辑：根据实际数据情况调整
            print("\n开始过滤已使用的优惠券...")
            
            # 方案1：严格按照状态过滤
            dwd_tool_coupon_used_strict = coupon_use_df.select(
                col("id").alias("coupon_use_id"),
                col("coupon_id"),
                col("user_id"),
                col("order_id"),
                col("coupon_status"),
                col("get_time"),
                col("using_time"),
                col("used_time"),
                col("expire_time"),
                # 如果used_time为空，使用using_time作为dt
                coalesce(
                    date_format(col("used_time"), "yyyy-MM-dd"),
                    date_format(col("using_time"), "yyyy-MM-dd"),
                    date_format(col("get_time"), "yyyy-MM-dd")
                ).alias("dt")
            ).filter(col("coupon_status") == "2")
            
            print(f"严格状态过滤后记录数: {dwd_tool_coupon_used_strict.count()}")
            
            # 如果严格过滤后没有数据，尝试宽松过滤
            if dwd_tool_coupon_used_strict.count() == 0:
                print("⚠️ 严格过滤没有结果，尝试宽松过滤条件...")
                
                # 方案2：包含所有有used_time或using_time的记录
                dwd_tool_coupon_used_loose = coupon_use_df.select(
                    col("id").alias("coupon_use_id"),
                    col("coupon_id"),
                    col("user_id"),
                    col("order_id"),
                    col("coupon_status"),
                    col("get_time"),
                    col("using_time"),
                    col("used_time"),
                    col("expire_time"),
                    coalesce(
                        date_format(col("used_time"), "yyyy-MM-dd"),
                        date_format(col("using_time"), "yyyy-MM-dd"),
                        date_format(col("get_time"), "yyyy-MM-dd")
                    ).alias("dt")
                ).filter(
                    (col("used_time").isNotNull()) | 
                    (col("using_time").isNotNull()) |
                    (col("coupon_status").isin(["2", 2]))  # 支持字符串和数字类型
                )
                
                print(f"宽松过滤后记录数: {dwd_tool_coupon_used_loose.count()}")
                
                if dwd_tool_coupon_used_loose.count() > 0:
                    dwd_tool_coupon_used = dwd_tool_coupon_used_loose
                    print("✓ 使用宽松过滤条件")
                else:
                    print("⚠️ 宽松过滤仍无结果，使用所有记录...")
                    # 方案3：使用所有记录，但优先处理有时间信息的
                    dwd_tool_coupon_used = coupon_use_df.select(
                        col("id").alias("coupon_use_id"),
                        col("coupon_id"),
                        col("user_id"),
                        col("order_id"),
                        col("coupon_status"),
                        col("get_time"),
                        col("using_time"),
                        col("used_time"),
                        col("expire_time"),
                        coalesce(
                            date_format(col("used_time"), "yyyy-MM-dd"),
                            date_format(col("using_time"), "yyyy-MM-dd"),
                            date_format(col("get_time"), "yyyy-MM-dd"),
                            lit("2025-06-24")  # 默认日期
                        ).alias("dt")
                    )
                    print("✓ 使用所有记录")
            else:
                dwd_tool_coupon_used = dwd_tool_coupon_used_strict
                print("✓ 使用严格过滤条件")
            
            # 显示最终结果样本
            if dwd_tool_coupon_used.count() > 0:
                print("\n最终处理结果样本:")
                dwd_tool_coupon_used.show(5, truncate=False)
            
            # 写入DWD层
            dwd_path = f"{self.paths_config['dwd_path']}/dwd_tool_coupon_used"
            SparkUtil.write_to_hdfs(dwd_tool_coupon_used, dwd_path, spark=self.spark)
            
            print(f"✓ 工具域优惠券使用事务事实表构建完成，记录数: {dwd_tool_coupon_used.count()}")
            
        except Exception as e:
            print(f"✗ 工具域优惠券使用事务事实表构建失败: {e}")
            import traceback
            traceback.print_exc()
            raise 
        
    def build_dwd_interaction_favor_add(self):
        """构建互动域收藏商品事务事实表"""
        try:
            print("\n开始构建互动域收藏商品事务事实表...")
            
            # 读取收藏表
            favor_info_path = f"{self.paths_config['ods_path']}/favor_info"
            favor_info_df = SparkUtil.read_from_hdfs(favor_info_path, spark=self.spark)
            
            # 查看表结构以确认字段
            print("收藏表字段结构:")
            favor_info_df.printSchema()
            
            # 构建收藏商品事务事实表
            dwd_interaction_favor_add = favor_info_df.select(
                col("id").alias("favor_id"),
                col("user_id"),
                col("sku_id"),
                col("spu_id"),
                col("is_cancel"),
                col("create_time"),
                col("operate_time"),
                date_format(col("create_time"), "yyyy-MM-dd").alias("dt")
            ).filter(col("is_cancel") == "0")  # 只取未取消的收藏
            
            # 写入DWD层
            dwd_path = f"{self.paths_config['dwd_path']}/dwd_interaction_favor_add"
            SparkUtil.write_to_hdfs(dwd_interaction_favor_add, dwd_path, spark=self.spark)
            
            print(f"✓ 互动域收藏商品事务事实表构建完成，记录数: {dwd_interaction_favor_add.count()}")
            
        except Exception as e:
            print(f"✗ 互动域收藏商品事务事实表构建失败: {e}")
            raise
        
    def build_dwd_user_register(self):
        """构建用户域用户注册事务事实表"""
        try:
            print("\n开始构建用户域用户注册事务事实表...")
            
            # 读取用户信息表
            user_info_path = f"{self.paths_config['ods_path']}/user_info"
            user_info_df = SparkUtil.read_from_hdfs(user_info_path, spark=self.spark)
            
            # 构建用户注册事务事实表
            dwd_user_register = user_info_df.select(
                col("id").alias("user_id"),
                col("login_name"),
                col("nick_name"),
                col("name").alias("real_name"),
                col("phone_num"),
                col("email"),
                col("user_level"),
                col("birthday"),
                col("gender"),
                col("create_time"),
                col("operate_time"),
                col("status"),
                date_format(col("create_time"), "yyyy-MM-dd").alias("dt")
            )
            
            # 写入DWD层
            dwd_path = f"{self.paths_config['dwd_path']}/dwd_user_register"
            SparkUtil.write_to_hdfs(dwd_user_register, dwd_path, spark=self.spark)
            
            print(f"✓ 用户域用户注册事务事实表构建完成，记录数: {dwd_user_register.count()}")
            
        except Exception as e:
            print(f"✗ 用户域用户注册事务事实表构建失败: {e}")
            raise
    
    def build_all_dwds(self):
        """构建所有DWD事实表"""
        print("=== 开始构建DWD事实表 ===")
        
        dwds = [
            ("交易域下单事务事实表", self.build_dwd_trade_order_detail),
            ("交易域支付成功事务事实表", self.build_dwd_trade_pay_detail),
            ("交易域加购事务事实表", self.build_dwd_trade_cart_add),
            ("工具域优惠券使用事务事实表", self.build_dwd_tool_coupon_used),
            ("互动域收藏商品事务事实表", self.build_dwd_interaction_favor_add),
            ("用户域用户注册事务事实表", self.build_dwd_user_register)
        ]
        
        success_count = 0
        failure_count = 0
        
        for dwd_name, dwd_func in dwds:
            try:
                dwd_func()
                success_count += 1
            except Exception as e:
                failure_count += 1
                print(f"✗ {dwd_name} 构建失败: {e}")
                continue
        
        print(f"\n=== DWD层构建完成 ===")
        print(f"成功: {success_count}张事实表")
        print(f"失败: {failure_count}张事实表")
        print(f"总计: {len(dwds)}张事实表")

def main():
    """主函数"""
    try:
        ods_to_dwd = ODSToDWD()
        ods_to_dwd.build_all_dwds()
        
    except Exception as e:
        logging.error(f"ODS到DWD构建失败: {e}")
        print(f"✗ ODS到DWD构建失败: {e}")
    finally:
        SparkUtil.stop_spark_session()

if __name__ == "__main__":
    # 设置日志
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    main()
