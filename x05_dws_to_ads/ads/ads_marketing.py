"""
ADS - 营销/工具相关指标
包含：
- 优惠券使用统计
"""
from tools.spark_util import SparkUtil
from pyspark.sql.functions import *


def build_coupon_stats(spark, paths_config):
    print("\n开始构建优惠券使用统计...")

    dws_tool_user_coupon_path = f"{paths_config['dws_path']}/dws_tool_user_coupon_coupon_used_nd"
    dws_tool_user_coupon_df = SparkUtil.read_from_hdfs(dws_tool_user_coupon_path, spark=spark)

    dim_coupon_path = f"{paths_config['dim_path']}/dim_coupon"
    dim_coupon_df = SparkUtil.read_from_hdfs(dim_coupon_path, spark=spark)

    ads_coupon_stats = dws_tool_user_coupon_df.alias("dws") \
        .join(dim_coupon_df.alias("coupon"), col("dws.coupon_id") == col("coupon.coupon_id"), "inner") \
        .groupBy("coupon.coupon_id", "coupon.coupon_name", "coupon.coupon_type") \
        .agg(
            sum("dws.used_count").alias("used_count"),
            countDistinct("dws.user_id").alias("used_user_count")
        ) \
        .select(
            col("coupon_id"),
            col("coupon_name"),
            col("coupon_type"),
            col("used_count"),
            col("used_user_count"),
            current_date().alias("dt")
        )

    SparkUtil.write_to_mysql(ads_coupon_stats, "ads_coupon_stats", spark=spark)
    print(f"✓ 优惠券使用统计构建完成，记录数: {ads_coupon_stats.count()}")
