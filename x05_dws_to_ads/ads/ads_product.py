"""
ADS - 商品相关指标
包含：
- 商品复购率
"""
from tools.spark_util import SparkUtil
from pyspark.sql.functions import *


def build_sku_repurchase_rate(spark, paths_config):
    print("\n开始构建商品复购率指标...")

    dws_trade_user_sku_order_path = f"{paths_config['dws_path']}/dws_trade_user_sku_order_nd"
    dws_trade_user_sku_order_df = SparkUtil.read_from_hdfs(dws_trade_user_sku_order_path, spark=spark)

    dim_sku_path = f"{paths_config['dim_path']}/dim_sku"
    dim_sku_df = SparkUtil.read_from_hdfs(dim_sku_path, spark=spark)

    dws_trade_user_sku_order_df.createOrReplaceTempView("trade_user_sku_order")
    dim_sku_df.createOrReplaceTempView("dim_sku")

    repurchase_sql = """
    WITH user_sku_purchase AS (
        SELECT 
            user_id,
            sku_id,
            order_count,
            order_num,
            order_amount
        FROM trade_user_sku_order
        WHERE order_count > 0
    ),
    sku_repurchase_stats AS (
        SELECT 
            sku_id,
            COUNT(DISTINCT user_id) AS total_buyers,
            COUNT(DISTINCT CASE 
                WHEN order_count >= 2 THEN user_id 
            END) AS repurchase_users,
            SUM(order_count) AS total_orders,
            SUM(order_amount) AS total_amount
        FROM user_sku_purchase
        GROUP BY sku_id
        HAVING COUNT(DISTINCT user_id) >= 2
    )
    SELECT 
        srs.sku_id,
        sku.sku_name,
        sku.tm_name,
        sku.category1_name,
        sku.category2_name,
        sku.category3_name,
        srs.total_buyers,
        srs.repurchase_users,
        srs.total_orders,
        ROUND(srs.total_amount, 2) AS total_amount,
        ROUND(
            CASE 
                WHEN srs.total_buyers > 0 THEN srs.repurchase_users * 100.0 / srs.total_buyers 
                ELSE 0 
            END, 2
        ) AS repurchase_rate,
        current_date() AS dt
    FROM sku_repurchase_stats srs
    JOIN dim_sku sku ON srs.sku_id = sku.sku_id
    WHERE srs.repurchase_users > 0
    ORDER BY repurchase_rate DESC, total_buyers DESC
    LIMIT 100
    """

    ads_sku_repurchase = spark.sql(repurchase_sql)
    SparkUtil.write_to_mysql(ads_sku_repurchase, "ads_sku_repurchase_rate", mode="overwrite", spark=spark)
    print(f"✓ 商品复购率指标构建完成，记录数: {ads_sku_repurchase.count()}")
