"""
ADS - 收藏相关指标
包含：
- 商品收藏排行榜TOP10
"""
from tools.spark_util import SparkUtil
from pyspark.sql.functions import *


def build_sku_favor_count_top10(spark, paths_config):
    print("\n开始构建商品收藏排行榜TOP10...")

    dws_interaction_sku_favor_path = f"{paths_config['dws_path']}/dws_interaction_sku_favor_add_nd"
    dws_interaction_sku_favor_df = SparkUtil.read_from_hdfs(dws_interaction_sku_favor_path, spark=spark)

    dim_sku_path = f"{paths_config['dim_path']}/dim_sku"
    dim_sku_df = SparkUtil.read_from_hdfs(dim_sku_path, spark=spark)

    ads_sku_favor_count_top10 = dws_interaction_sku_favor_df.alias("dws") \
        .join(dim_sku_df.alias("sku"), col("dws.sku_id") == col("sku.sku_id"), "inner") \
        .select(
            col("dws.sku_id"),
            col("sku.sku_name"),
            col("sku.tm_name"),
            col("sku.category1_name"),
            col("sku.category2_name"),
            col("sku.category3_name"),
            col("dws.favor_count"),
            col("dws.favor_user_count"),
            current_date().alias("dt")
        ) \
        .orderBy(col("favor_count").desc()) \
        .limit(10)

    SparkUtil.write_to_mysql(ads_sku_favor_count_top10, "ads_sku_favor_count_top10", spark=spark)
    print(f"✓ 商品收藏排行榜TOP10构建完成，记录数: {ads_sku_favor_count_top10.count()}")
