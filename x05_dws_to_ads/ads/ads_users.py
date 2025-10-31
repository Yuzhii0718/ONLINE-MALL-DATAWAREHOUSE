"""
ADS - 用户相关指标
包含：
- 用户总体统计
- 用户变动统计（活跃、流失、回流）
- 用户留存率
"""
from tools.spark_util import SparkUtil
from pyspark.sql.functions import *


def build_user_stats(spark, paths_config):
    print("\n开始构建用户统计...")

    dws_trade_user_order_path = f"{paths_config['dws_path']}/dws_trade_user_order_nd"
    dws_trade_user_order_df = SparkUtil.read_from_hdfs(dws_trade_user_order_path, spark=spark)

    dws_user_user_login_path = f"{paths_config['dws_path']}/dws_user_user_login_nd"
    dws_user_user_login_df = SparkUtil.read_from_hdfs(dws_user_user_login_path, spark=spark)

    dws_trade_user_order_df.createOrReplaceTempView("dws_trade_user_order")
    dws_user_user_login_df.createOrReplaceTempView("dws_user_user_login")

    ads_user_stats = spark.sql(
        """
        SELECT 
            metric_name,
            CAST(metric_value AS DOUBLE) as metric_value,
            current_date() as dt
        FROM (
            SELECT 'total_user_count' as metric_name, COUNT(*) as metric_value
            FROM dws_user_user_login
            
            UNION ALL
            
            SELECT 'order_user_count' as metric_name, COUNT(*) as metric_value
            FROM dws_trade_user_order
            
            UNION ALL
            
            SELECT 'total_order_count' as metric_name, COALESCE(SUM(order_count), 0) as metric_value
            FROM dws_trade_user_order
            
            UNION ALL
            
            SELECT 'total_order_amount' as metric_name, COALESCE(SUM(order_amount), 0) as metric_value
            FROM dws_trade_user_order
        ) user_stats
        """
    )

    SparkUtil.write_to_mysql(ads_user_stats, "ads_user_stats", spark=spark)
    print(f"✓ 用户统计构建完成，记录数: {ads_user_stats.count()}")


def build_user_change(spark, paths_config):
    print("\n开始构建用户变动统计表...")

    dws_user_login_path = f"{paths_config['dws_path']}/dws_user_user_login_nd"
    dws_user_login_df = SparkUtil.read_from_hdfs(dws_user_login_path, spark=spark)

    dws_user_login_df.createOrReplaceTempView("user_login_summary")

    user_change_sql = """
    WITH date_calculations AS (
        SELECT 
            user_id,
            last_login_time,
            datediff(current_date(), last_login_time) as days_since_last_login,
            CASE WHEN date_format(last_login_time, 'yyyy-MM-dd') = current_date() THEN 1 ELSE 0 END as is_today_login
        FROM user_login_summary
    ),
    user_stats AS (
        SELECT
            SUM(CASE WHEN days_since_last_login > 3 THEN 1 ELSE 0 END) as churn_count,
            SUM(CASE 
                WHEN is_today_login = 1 AND days_since_last_login >= 3 THEN 1 
                ELSE 0 
            END) as back_count
        FROM date_calculations
    )
    SELECT 
        current_date() as dt,
        CAST(COALESCE(churn_count, 0) AS INT) as user_churn_count,
        CAST(COALESCE(back_count, 0) AS INT) as user_back_count
    FROM user_stats
    """

    ads_user_change = spark.sql(user_change_sql)
    SparkUtil.write_to_mysql(ads_user_change, "ads_user_change", mode="overwrite", spark=spark)
    print(f"✓ 用户变动统计表构建完成，记录数: {ads_user_change.count()}")


def build_user_retention(spark, paths_config):
    print("\n开始构建用户留存率指标...")

    dws_user_login_path = f"{paths_config['dws_path']}/dws_user_user_login_nd"
    dws_user_login_df = SparkUtil.read_from_hdfs(dws_user_login_path, spark=spark)

    dws_user_login_df.createOrReplaceTempView("user_login")

    retention_sql = """
    WITH user_first_login AS (
        SELECT 
            user_id,
            MIN(last_login_time) AS first_login_date,
            MAX(last_login_time) AS latest_login_date
        FROM user_login
        GROUP BY user_id
    ),
    cohort_analysis AS (
        SELECT 
            date_format(first_login_date, 'yyyy-MM-dd') AS register_date,
            COUNT(DISTINCT user_id) AS new_users,
            COUNT(DISTINCT CASE WHEN datediff(latest_login_date, first_login_date) >= 1 THEN user_id END) AS day1_retained,
            COUNT(DISTINCT CASE WHEN datediff(latest_login_date, first_login_date) >= 7 THEN user_id END) AS week1_retained,
            COUNT(DISTINCT CASE WHEN datediff(latest_login_date, first_login_date) >= 30 THEN user_id END) AS month1_retained
        FROM user_first_login
        GROUP BY date_format(first_login_date, 'yyyy-MM-dd')
    )
    SELECT 
        register_date,
        new_users,
        day1_retained,
        week1_retained,
        month1_retained,
        ROUND(CASE WHEN new_users > 0 THEN day1_retained * 100.0 / new_users ELSE 0 END, 2) AS day1_retention_rate,
        ROUND(CASE WHEN new_users > 0 THEN week1_retained * 100.0 / new_users ELSE 0 END, 2) AS week1_retention_rate,
        ROUND(CASE WHEN new_users > 0 THEN month1_retained * 100.0 / new_users ELSE 0 END, 2) AS month1_retention_rate,
        current_date() AS dt
    FROM cohort_analysis
    WHERE new_users > 0
    ORDER BY register_date
    """

    ads_user_retention = spark.sql(retention_sql)
    SparkUtil.write_to_mysql(ads_user_retention, "ads_user_retention", mode="overwrite", spark=spark)
    print(f"✓ 用户留存率指标构建完成，记录数: {ads_user_retention.count()}")
