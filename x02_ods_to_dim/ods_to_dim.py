"""
ODS到DIM层维度表构建
从ODS层数据构建各维度表
"""
import logging
from tools.spark_util import SparkUtil
from config.config_manager import ConfigManager
from pyspark.sql.functions import *
from pyspark.sql.types import *

class ODSToDIM:
    def __init__(self):
        self.spark = SparkUtil.get_spark_session("ODS-To-DIM")
        self.config_manager = ConfigManager()
        self.paths_config = self.config_manager.get_paths_config()
    
    def build_dim_date(self):
        """构建日期维度表"""
        try:
            print("\n开始构建日期维度表...")
            
            # 生成2020-2030年的日期维度数据
            from datetime import datetime, timedelta
            import pandas as pd
            
            start_date = datetime(2020, 1, 1)
            end_date = datetime(2030, 12, 31)
            
            dates = []
            current_date = start_date
            while current_date <= end_date:
                dates.append({
                    'date_id': current_date.strftime('%Y%m%d'),
                    'date_name': current_date.strftime('%Y-%m-%d'),
                    'year_id': current_date.year,
                    'year_name': str(current_date.year),
                    'month_id': current_date.month,
                    'month_name': current_date.strftime('%Y-%m'),
                    'day_id': current_date.day,
                    'day_name': current_date.strftime('%Y-%m-%d'),
                    'week_id': current_date.isocalendar()[1],
                    'week_day': current_date.weekday() + 1,
                    'week_day_name': current_date.strftime('%A'),
                    'quarter_id': (current_date.month - 1) // 3 + 1,
                    'quarter_name': f"{current_date.year}Q{(current_date.month - 1) // 3 + 1}",
                    'is_weekend': 1 if current_date.weekday() >= 5 else 0
                })
                current_date += timedelta(days=1)
            
            # 创建DataFrame
            date_df = self.spark.createDataFrame(dates)
            
            # 写入DIM层
            dim_path = f"{self.paths_config['dim_path']}/dim_date"
            SparkUtil.write_to_hdfs(date_df, dim_path, spark=self.spark)
            
            print(f"✓ 日期维度表构建完成，记录数: {date_df.count()}")
            
        except Exception as e:
            print(f"✗ 日期维度表构建失败: {e}")
            raise
    
    def build_dim_user(self):
        """构建用户维度表"""
        try:
            print("\n开始构建用户维度表...")
            
            # 读取用户信息表
            user_info_path = f"{self.paths_config['ods_path']}/user_info"
            user_info_df = SparkUtil.read_from_hdfs(user_info_path, spark=self.spark)
            
            # 先查看用户表的实际字段结构
            print("用户表字段结构:")
            user_info_df.printSchema()
            
            # 构建用户维度表（不包含省份信息，因为user_info表中没有province_id字段）
            dim_user_df = user_info_df.select(
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
                # 添加默认省份信息，后续可以通过其他方式获取
                lit(None).cast("string").alias("province_id"),
                lit(None).cast("string").alias("province_name"),
                lit(None).cast("string").alias("region_id"),
                lit(None).cast("string").alias("region_name")
            )
            
            # 写入DIM层
            dim_path = f"{self.paths_config['dim_path']}/dim_user"
            SparkUtil.write_to_hdfs(dim_user_df, dim_path, spark=self.spark)
            
            print(f"✓ 用户维度表构建完成，记录数: {dim_user_df.count()}")
            
        except Exception as e:
            print(f"✗ 用户维度表构建失败: {e}")
            raise
        
    def build_dim_sku(self):
        """构建商品维度表"""
        try:
            print("\n开始构建商品维度表...")
            
            # 读取SKU信息表
            sku_info_path = f"{self.paths_config['ods_path']}/sku_info"
            sku_info_df = SparkUtil.read_from_hdfs(sku_info_path, spark=self.spark)
            
            # 读取SPU信息表
            spu_info_path = f"{self.paths_config['ods_path']}/spu_info"
            spu_info_df = SparkUtil.read_from_hdfs(spu_info_path, spark=self.spark)
            
            # 读取品牌表
            trademark_path = f"{self.paths_config['ods_path']}/base_trademark"
            trademark_df = SparkUtil.read_from_hdfs(trademark_path, spark=self.spark)
            
            # 读取三级分类表
            category3_path = f"{self.paths_config['ods_path']}/base_category3"
            category3_df = SparkUtil.read_from_hdfs(category3_path, spark=self.spark)
            
            # 读取二级分类表
            category2_path = f"{self.paths_config['ods_path']}/base_category2"
            category2_df = SparkUtil.read_from_hdfs(category2_path, spark=self.spark)
            
            # 读取一级分类表
            category1_path = f"{self.paths_config['ods_path']}/base_category1"
            category1_df = SparkUtil.read_from_hdfs(category1_path, spark=self.spark)
            
            # 构建商品维度表
            dim_sku_df = sku_info_df.alias("sku") \
                .join(spu_info_df.alias("spu"), col("sku.spu_id") == col("spu.id"), "left") \
                .join(trademark_df.alias("tm"), col("sku.tm_id") == col("tm.id"), "left") \
                .join(category3_df.alias("c3"), col("sku.category3_id") == col("c3.id"), "left") \
                .join(category2_df.alias("c2"), col("c3.category2_id") == col("c2.id"), "left") \
                .join(category1_df.alias("c1"), col("c2.category1_id") == col("c1.id"), "left") \
                .select(
                    col("sku.id").alias("sku_id"),
                    col("sku.sku_name"),
                    col("sku.sku_desc"),
                    col("sku.price"),
                    col("sku.weight"),
                    col("sku.is_sale"),
                    col("sku.create_time"),
                    col("sku.operate_time"),
                    col("spu.id").alias("spu_id"),
                    col("spu.spu_name"),
                    col("spu.description").alias("spu_desc"),
                    col("tm.id").alias("tm_id"),
                    col("tm.tm_name"),
                    col("c3.id").alias("category3_id"),
                    col("c3.name").alias("category3_name"),
                    col("c2.id").alias("category2_id"),
                    col("c2.name").alias("category2_name"),
                    col("c1.id").alias("category1_id"),
                    col("c1.name").alias("category1_name")
                )
            
            # 写入DIM层
            dim_path = f"{self.paths_config['dim_path']}/dim_sku"
            SparkUtil.write_to_hdfs(dim_sku_df, dim_path, spark=self.spark)
            
            print(f"✓ 商品维度表构建完成，记录数: {dim_sku_df.count()}")
            
        except Exception as e:
            print(f"✗ 商品维度表构建失败: {e}")
            raise
    
    def build_dim_coupon(self):
        """构建优惠券维度表"""
        try:
            print("\n开始构建优惠券维度表...")
            
            # 读取优惠券信息表
            coupon_info_path = f"{self.paths_config['ods_path']}/coupon_info"
            coupon_info_df = SparkUtil.read_from_hdfs(coupon_info_path, spark=self.spark)
            
            # 构建优惠券维度表
            dim_coupon_df = coupon_info_df.select(
                col("id").alias("coupon_id"),
                col("coupon_name"),
                col("coupon_type"),
                col("condition_amount"),
                col("condition_num"),
                col("activity_id"),
                col("benefit_amount"),
                col("benefit_discount"),
                col("create_time"),
                col("range_type"),
                col("limit_num"),
                col("taken_count"),
                col("start_time"),
                col("end_time"),
                col("operate_time"),
                col("expire_time"),
                col("range_desc")
            )
            
            # 写入DIM层
            dim_path = f"{self.paths_config['dim_path']}/dim_coupon"
            SparkUtil.write_to_hdfs(dim_coupon_df, dim_path, spark=self.spark)
            
            print(f"✓ 优惠券维度表构建完成，记录数: {dim_coupon_df.count()}")
            
        except Exception as e:
            print(f"✗ 优惠券维度表构建失败: {e}")
            raise
    
    def build_dim_activity(self):
        """构建活动维度表"""
        try:
            print("\n开始构建活动维度表...")
            
            # 读取活动信息表
            activity_info_path = f"{self.paths_config['ods_path']}/activity_info"
            activity_info_df = SparkUtil.read_from_hdfs(activity_info_path, spark=self.spark)
            
            # 读取活动规则表
            activity_rule_path = f"{self.paths_config['ods_path']}/activity_rule"
            activity_rule_df = SparkUtil.read_from_hdfs(activity_rule_path, spark=self.spark)
            
            # 构建活动维度表
            dim_activity_df = activity_info_df.alias("ai") \
                .join(activity_rule_df.alias("ar"), col("ai.id") == col("ar.activity_id"), "left") \
                .select(
                    col("ai.id").alias("activity_id"),
                    col("ai.activity_name"),
                    col("ai.activity_type"),
                    col("ai.activity_desc"),
                    col("ai.start_time"),
                    col("ai.end_time"),
                    col("ai.create_time"),
                    col("ai.operate_time"),
                    col("ar.condition_amount"),
                    col("ar.condition_num"),
                    col("ar.benefit_amount"),
                    col("ar.benefit_discount"),
                    col("ar.benefit_level")
                )
            
            # 写入DIM层
            dim_path = f"{self.paths_config['dim_path']}/dim_activity"
            SparkUtil.write_to_hdfs(dim_activity_df, dim_path, spark=self.spark)
            
            print(f"✓ 活动维度表构建完成，记录数: {dim_activity_df.count()}")
            
        except Exception as e:
            print(f"✗ 活动维度表构建失败: {e}")
            raise
    
    def build_all_dims(self):
        """构建所有维度表"""
        print("=== 开始构建DIM维度表 ===")
        
        dims = [
            ("日期维度表", self.build_dim_date),
            ("用户维度表", self.build_dim_user),
            ("商品维度表", self.build_dim_sku),
            ("优惠券维度表", self.build_dim_coupon),
            ("活动维度表", self.build_dim_activity)
        ]
        
        success_count = 0
        failure_count = 0
        
        for dim_name, dim_func in dims:
            try:
                dim_func()
                success_count += 1
            except Exception as e:
                failure_count += 1
                print(f"✗ {dim_name} 构建失败: {e}")
                continue
        
        print(f"\n=== DIM层构建完成 ===")
        print(f"成功: {success_count}张维度表")
        print(f"失败: {failure_count}张维度表")
        print(f"总计: {len(dims)}张维度表")

def main():
    """主函数"""
    try:
        ods_to_dim = ODSToDIM()
        ods_to_dim.build_all_dims()
        
    except Exception as e:
        logging.error(f"ODS到DIM构建失败: {e}")
        print(f"✗ ODS到DIM构建失败: {e}")
    finally:
        SparkUtil.stop_spark_session()

if __name__ == "__main__":
    # 设置日志
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    main()
