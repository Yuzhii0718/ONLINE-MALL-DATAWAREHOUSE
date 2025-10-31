"""
ADS - 订单相关指标
包含：
- 各品牌商品交易统计
- 各分类商品交易统计
- 各省份交易统计
- 下单到支付时间间隔平均值
"""
from tools.spark_util import SparkUtil
from pyspark.sql.functions import *


def build_trade_stats_by_tm(spark, paths_config):
    """各品牌商品交易统计"""
    print("\n开始构建各品牌商品交易统计...")

    dws_trade_sku_order_path = f"{paths_config['dws_path']}/dws_trade_sku_order_nd"
    dws_trade_sku_order_df = SparkUtil.read_from_hdfs(dws_trade_sku_order_path, spark=spark)

    dim_sku_path = f"{paths_config['dim_path']}/dim_sku"
    dim_sku_df = SparkUtil.read_from_hdfs(dim_sku_path, spark=spark)

    ads_trade_stats_by_tm = dws_trade_sku_order_df.alias("dws") \
        .join(dim_sku_df.alias("sku"), col("dws.sku_id") == col("sku.sku_id"), "inner") \
        .groupBy("sku.tm_id", "sku.tm_name") \
        .agg(
            sum("dws.order_count").alias("order_count"),
            sum("dws.user_count").alias("user_count"),
            sum("dws.order_num").alias("order_num"),
            sum("dws.order_amount").alias("order_amount"),
            sum("dws.activity_reduce_amount").alias("activity_reduce_amount"),
            sum("dws.coupon_reduce_amount").alias("coupon_reduce_amount")
        ) \
        .select(
            col("tm_id"),
            col("tm_name"),
            col("order_count"),
            col("user_count"),
            col("order_num"),
            col("order_amount"),
            col("activity_reduce_amount"),
            col("coupon_reduce_amount"),
            current_date().alias("dt")
        )

    SparkUtil.write_to_mysql(ads_trade_stats_by_tm, "ads_trade_stats_by_tm", spark=spark)
    print(f"✓ 各品牌商品交易统计构建完成，记录数: {ads_trade_stats_by_tm.count()}")


def build_trade_stats_by_cate(spark, paths_config):
    """各分类商品交易统计（一级分类）"""
    print("\n开始构建各分类商品交易统计...")

    dws_trade_sku_order_path = f"{paths_config['dws_path']}/dws_trade_sku_order_nd"
    dws_trade_sku_order_df = SparkUtil.read_from_hdfs(dws_trade_sku_order_path, spark=spark)

    dim_sku_path = f"{paths_config['dim_path']}/dim_sku"
    dim_sku_df = SparkUtil.read_from_hdfs(dim_sku_path, spark=spark)

    ads_trade_stats_by_cate1 = dws_trade_sku_order_df.alias("dws") \
        .join(dim_sku_df.alias("sku"), col("dws.sku_id") == col("sku.sku_id"), "inner") \
        .groupBy("sku.category1_id", "sku.category1_name") \
        .agg(
            sum("dws.order_count").alias("order_count"),
            sum("dws.user_count").alias("user_count"),
            sum("dws.order_num").alias("order_num"),
            sum("dws.order_amount").alias("order_amount"),
            sum("dws.activity_reduce_amount").alias("activity_reduce_amount"),
            sum("dws.coupon_reduce_amount").alias("coupon_reduce_amount")
        ) \
        .select(
            col("category1_id").alias("category_id"),
            col("category1_name").alias("category_name"),
            lit(1).alias("category_level"),
            col("order_count"),
            col("user_count"),
            col("order_num"),
            col("order_amount"),
            col("activity_reduce_amount"),
            col("coupon_reduce_amount"),
            current_date().alias("dt")
        )

    SparkUtil.write_to_mysql(ads_trade_stats_by_cate1, "ads_trade_stats_by_cate", spark=spark)
    print(f"✓ 各分类商品交易统计构建完成，记录数: {ads_trade_stats_by_cate1.count()}")


def build_trade_stats_by_province(spark, paths_config):
    """各省份交易统计"""
    print("\n开始构建各省份交易统计...")

    dws_trade_province_order_path = f"{paths_config['dws_path']}/dws_trade_province_order_nd"
    dws_trade_province_order_df = SparkUtil.read_from_hdfs(dws_trade_province_order_path, spark=spark)

    base_province_path = f"{paths_config['ods_path']}/base_province"
    base_province_df = SparkUtil.read_from_hdfs(base_province_path, spark=spark)

    print(f"DWS地区粒度表记录数: {dws_trade_province_order_df.count()}")
    print(f"省份表记录数: {base_province_df.count()}")

    ads_trade_stats_by_province = dws_trade_province_order_df.alias("dws") \
        .join(base_province_df.alias("bp"), col("dws.province_id") == col("bp.id"), "inner") \
        .select(
            col("dws.province_id"),
            col("bp.name").alias("province_name"),
            col("bp.region_id"),
            col("bp.area_code"),
            col("bp.iso_code"),
            col("bp.iso_3166_2"),
            col("dws.order_count"),
            col("dws.user_count"),
            col("dws.order_num"),
            col("dws.order_amount"),
            col("dws.activity_reduce_amount"),
            col("dws.coupon_reduce_amount"),
            current_date().alias("dt")
        )

    if ads_trade_stats_by_province.count() == 0:
        # 回退：字符串强制转换关联
        ads_trade_stats_by_province = dws_trade_province_order_df.alias("dws") \
            .join(base_province_df.alias("bp"), 
                  col("dws.province_id").cast("string") == col("bp.id").cast("string"), "inner") \
            .select(
                col("dws.province_id"),
                col("bp.name").alias("province_name"),
                col("bp.region_id"),
                col("bp.area_code"),
                col("bp.iso_code"),
                col("bp.iso_3166_2"),
                col("dws.order_count"),
                col("dws.user_count"),
                col("dws.order_num"),
                col("dws.order_amount"),
                col("dws.activity_reduce_amount"),
                col("dws.coupon_reduce_amount"),
                current_date().alias("dt")
            )

    if ads_trade_stats_by_province.count() > 0:
        SparkUtil.write_to_mysql(ads_trade_stats_by_province, "ads_trade_stats_by_province", spark=spark)
        print(f"✓ 各省份交易统计构建完成，记录数: {ads_trade_stats_by_province.count()}")
    else:
        print("❌ 无法产生有效的省份交易统计数据")


def build_order_to_pay_interval_avg(spark, paths_config):
    """下单到支付时间间隔平均值"""
    print("\n开始构建下单到支付时间间隔平均值指标表...")

    dwd_trade_pay_detail_path = f"{paths_config['dwd_path']}/dwd_trade_pay_detail"
    pay_detail_df = SparkUtil.read_from_hdfs(dwd_trade_pay_detail_path, spark=spark)

    order_info_path = f"{paths_config['ods_path']}/order_info"
    order_info_df = SparkUtil.read_from_hdfs(order_info_path, spark=spark)

    order_pay_join = pay_detail_df.alias("pay") \
        .join(order_info_df.alias("order"), col("pay.order_id") == col("order.id"), "inner") \
        .select(
            col("pay.order_id"),
            col("order.create_time").alias("order_create_time"),
            col("pay.callback_time").alias("pay_callback_time"),
            col("pay.dt")
        ) \
        .filter((col("order_create_time").isNotNull()) & (col("pay_callback_time").isNotNull()))

    if order_pay_join.count() == 0:
        print("❌ 订单和支付信息关联失败，没有匹配的记录")
        return

    order_pay_interval = order_pay_join \
        .withColumn("order_to_pay_interval_seconds",
                   unix_timestamp(col("pay_callback_time")) - unix_timestamp(col("order_create_time"))) \
        .withColumn("order_to_pay_interval",
                   col("order_to_pay_interval_seconds") / 60.0) \
        .filter(col("order_to_pay_interval") >= 0)

    ads_order_to_pay_interval_avg = order_pay_interval \
        .groupBy("dt") \
        .agg(
            avg("order_to_pay_interval").alias("order_to_pay_interval_avg"),
            count("*").alias("order_count"),
            min("order_to_pay_interval").alias("min_interval"),
            max("order_to_pay_interval").alias("max_interval")
        ) \
        .select(
            col("dt"),
            col("order_to_pay_interval_avg").cast("decimal(10,2)").alias("order_to_pay_interval_avg"),
            col("order_count"),
            col("min_interval").cast("decimal(10,2)").alias("min_interval"),
            col("max_interval").cast("decimal(10,2)").alias("max_interval")
        ) \
        .orderBy("dt")

    if ads_order_to_pay_interval_avg.count() > 0:
        SparkUtil.write_to_mysql(ads_order_to_pay_interval_avg, "ads_order_to_pay_interval_avg", mode="overwrite", spark=spark)
        print(f"✓ 下单到支付时间间隔平均值指标表构建完成，记录数: {ads_order_to_pay_interval_avg.count()}")
    else:
        print("❌ 没有生成任何统计结果")
