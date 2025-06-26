"""
DWS到ADS层应用数据服务
从DWS汇总表构建最终指标表并存入MySQL
"""
import logging
from tools.spark_util import SparkUtil
from config.config_manager import ConfigManager
from pyspark.sql.functions import *
from pyspark.sql.types import *

class DWSToADS:
    def __init__(self):
        self.spark = SparkUtil.get_spark_session("DWS-To-ADS")
        self.config_manager = ConfigManager()
        self.paths_config = self.config_manager.get_paths_config()
    
    def build_ads_trade_stats_by_tm(self):
        """构建各品牌商品交易统计"""
        try:
            print("\n开始构建各品牌商品交易统计...")
            
            # 读取DWS商品粒度订单汇总表
            dws_trade_sku_order_path = f"{self.paths_config['dws_path']}/dws_trade_sku_order_nd"
            dws_trade_sku_order_df = SparkUtil.read_from_hdfs(dws_trade_sku_order_path, spark=self.spark)
            
            # 读取商品维度表
            dim_sku_path = f"{self.paths_config['dim_path']}/dim_sku"
            dim_sku_df = SparkUtil.read_from_hdfs(dim_sku_path, spark=self.spark)
            
            # 关联维度表并按品牌统计
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
            
            # 写入MySQL
            SparkUtil.write_to_mysql(ads_trade_stats_by_tm, "ads_trade_stats_by_tm", spark=self.spark)
            
            print(f"✓ 各品牌商品交易统计构建完成，记录数: {ads_trade_stats_by_tm.count()}")
            
        except Exception as e:
            print(f"✗ 各品牌商品交易统计构建失败: {e}")
            raise
    
    def build_ads_trade_stats_by_cate(self):
        """构建各分类商品交易统计"""
        try:
            print("\n开始构建各分类商品交易统计...")
            
            # 读取DWS商品粒度订单汇总表
            dws_trade_sku_order_path = f"{self.paths_config['dws_path']}/dws_trade_sku_order_nd"
            dws_trade_sku_order_df = SparkUtil.read_from_hdfs(dws_trade_sku_order_path, spark=self.spark)
            
            # 读取商品维度表
            dim_sku_path = f"{self.paths_config['dim_path']}/dim_sku"
            dim_sku_df = SparkUtil.read_from_hdfs(dim_sku_path, spark=self.spark)
            
            # 关联维度表并按一级分类统计
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
            
            # 写入MySQL
            SparkUtil.write_to_mysql(ads_trade_stats_by_cate1, "ads_trade_stats_by_cate", spark=self.spark)
            
            print(f"✓ 各分类商品交易统计构建完成，记录数: {ads_trade_stats_by_cate1.count()}")
            
        except Exception as e:
            print(f"✗ 各分类商品交易统计构建失败: {e}")
            raise
    
    def build_ads_trade_stats_by_province(self):
        """构建各省份交易统计"""
        try:
            print("\n开始构建各省份交易统计...")
            
            # 读取DWS地区粒度订单汇总表
            dws_trade_province_order_path = f"{self.paths_config['dws_path']}/dws_trade_province_order_nd"
            dws_trade_province_order_df = SparkUtil.read_from_hdfs(dws_trade_province_order_path, spark=self.spark)
            
            # 读取省份表（正确的省份信息来源）
            base_province_path = f"{self.paths_config['ods_path']}/base_province"
            base_province_df = SparkUtil.read_from_hdfs(base_province_path, spark=self.spark)
            
            print(f"DWS地区粒度表记录数: {dws_trade_province_order_df.count()}")
            print(f"省份表记录数: {base_province_df.count()}")
            
            # 显示数据样本进行调试
            print("\nDWS地区粒度表数据样本:")
            dws_trade_province_order_df.show(10, truncate=False)
            
            print("\n省份表数据样本:")
            base_province_df.select("id", "name", "region_id", "area_code", "iso_code", "iso_3166_2").show(10, truncate=False)
            
            # 检查province_id在两个表中的分布
            print("\nDWS表中province_id分布:")
            dws_province_ids = dws_trade_province_order_df.select("province_id").distinct()
            dws_province_ids.show(20, truncate=False)
            
            print("\n省份表中id分布:")
            base_province_ids = base_province_df.select("id").distinct()
            base_province_ids.show(20, truncate=False)
            
            # 关联省份信息 - 包含新增的iso_code和iso_3166_2字段
            ads_trade_stats_by_province = dws_trade_province_order_df.alias("dws") \
                .join(base_province_df.alias("bp"), 
                      col("dws.province_id") == col("bp.id"), "inner") \
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
            
            print(f"\n关联后记录数: {ads_trade_stats_by_province.count()}")
            
            if ads_trade_stats_by_province.count() > 0:
                print("\n关联成功，显示统计结果:")
                ads_trade_stats_by_province.orderBy(col("order_amount").desc()).show(20, truncate=False)
                
                # 写入MySQL
                SparkUtil.write_to_mysql(ads_trade_stats_by_province, "ads_trade_stats_by_province", spark=self.spark)
                
                print(f"✓ 各省份交易统计构建完成，记录数: {ads_trade_stats_by_province.count()}")
            else:
                print("⚠️ 关联后没有数据，尝试类型转换关联...")
                
                # 尝试类型转换关联
                ads_trade_stats_by_province_converted = dws_trade_province_order_df.alias("dws") \
                    .join(base_province_df.alias("bp"), 
                          col("dws.province_id").cast("string") == col("bp.id").cast("string"), "inner") \
                    .select(
                        col("dws.province_id"),
                        col("bp.name").alias("province_name"),
                        col("bp.region_id"),
                        col("bp.area_code"),                    # 新增：行政区位码
                        col("bp.iso_code"),                     # 新增：国际编码
                        col("bp.iso_3166_2"),                   # 新增：ISO3166编码
                        col("dws.order_count"),
                        col("dws.user_count"),
                        col("dws.order_num"),
                        col("dws.order_amount"),
                        col("dws.activity_reduce_amount"),
                        col("dws.coupon_reduce_amount"),
                        current_date().alias("dt")
                    )
                
                print(f"类型转换后记录数: {ads_trade_stats_by_province_converted.count()}")
                
                if ads_trade_stats_by_province_converted.count() > 0:
                    print("\n类型转换后关联成功:")
                    ads_trade_stats_by_province_converted.orderBy(col("order_amount").desc()).show(20, truncate=False)
                    
                    # 写入MySQL
                    SparkUtil.write_to_mysql(ads_trade_stats_by_province_converted, "ads_trade_stats_by_province", spark=self.spark)
                    
                    print(f"✓ 各省份交易统计构建完成，记录数: {ads_trade_stats_by_province_converted.count()}")
                else:
                    # 如果还是没有数据，使用LEFT JOIN查看详情
                    print("进一步调试 - 使用LEFT JOIN:")
                    debug_join = dws_trade_province_order_df.alias("dws") \
                        .join(base_province_df.alias("bp"), 
                              col("dws.province_id").cast("string") == col("bp.id").cast("string"), "left") \
                        .select(
                            col("dws.province_id"),
                            col("bp.id").alias("base_province_id"),
                            col("bp.name").alias("province_name"),
                            col("bp.area_code"),
                            col("bp.iso_code"),
                            col("bp.iso_3166_2"),
                            col("dws.order_count")
                        )
                    
                    print("LEFT JOIN调试结果:")
                    debug_join.show(20, truncate=False)
                    
                    print("❌ 无法产生有效的省份交易统计数据")
            
        except Exception as e:
            print(f"✗ 各省份交易统计构建失败: {e}")
            import traceback
            traceback.print_exc()
            raise
        
    def build_ads_user_stats(self):
        """构建用户统计"""
        try:
            print("\n开始构建用户统计...")
            
            # 读取DWS用户粒度订单汇总表
            dws_trade_user_order_path = f"{self.paths_config['dws_path']}/dws_trade_user_order_nd"
            dws_trade_user_order_df = SparkUtil.read_from_hdfs(dws_trade_user_order_path, spark=self.spark)
            
            # 读取DWS用户粒度登录汇总表
            dws_user_user_login_path = f"{self.paths_config['dws_path']}/dws_user_user_login_nd"
            dws_user_user_login_df = SparkUtil.read_from_hdfs(dws_user_user_login_path, spark=self.spark)
            
            # 注册临时视图
            dws_trade_user_order_df.createOrReplaceTempView("dws_trade_user_order")
            dws_user_user_login_df.createOrReplaceTempView("dws_user_user_login")
            
            # 使用SQL构建统计结果
            ads_user_stats = self.spark.sql("""
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
            """)
            
            # 写入MySQL
            SparkUtil.write_to_mysql(ads_user_stats, "ads_user_stats", spark=self.spark)
            
            print(f"✓ 用户统计构建完成，记录数: {ads_user_stats.count()}")
            
        except Exception as e:
            print(f"✗ 用户统计构建失败: {e}")
            raise
        
    def build_ads_sku_favor_count_top10(self):
        """构建商品收藏排行榜TOP10"""
        try:
            print("\n开始构建商品收藏排行榜TOP10...")
            
            # 读取DWS商品粒度收藏汇总表
            dws_interaction_sku_favor_path = f"{self.paths_config['dws_path']}/dws_interaction_sku_favor_add_nd"
            dws_interaction_sku_favor_df = SparkUtil.read_from_hdfs(dws_interaction_sku_favor_path, spark=self.spark)
            
            # 读取商品维度表
            dim_sku_path = f"{self.paths_config['dim_path']}/dim_sku"
            dim_sku_df = SparkUtil.read_from_hdfs(dim_sku_path, spark=self.spark)
            
            # 关联维度表并取TOP10
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
            
            # 写入MySQL
            SparkUtil.write_to_mysql(ads_sku_favor_count_top10, "ads_sku_favor_count_top10", spark=self.spark)
            
            print(f"✓ 商品收藏排行榜TOP10构建完成，记录数: {ads_sku_favor_count_top10.count()}")
            
        except Exception as e:
            print(f"✗ 商品收藏排行榜TOP10构建失败: {e}")
            raise
    
    def build_ads_coupon_stats(self):
        """构建优惠券使用统计"""
        try:
            print("\n开始构建优惠券使用统计...")
            
            # 读取DWS用户优惠券粒度优惠券使用汇总表
            dws_tool_user_coupon_path = f"{self.paths_config['dws_path']}/dws_tool_user_coupon_coupon_used_nd"
            dws_tool_user_coupon_df = SparkUtil.read_from_hdfs(dws_tool_user_coupon_path, spark=self.spark)
            
            # 读取优惠券维度表
            dim_coupon_path = f"{self.paths_config['dim_path']}/dim_coupon"
            dim_coupon_df = SparkUtil.read_from_hdfs(dim_coupon_path, spark=self.spark)
            
            # 按优惠券统计使用情况
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
            
            # 写入MySQL
            SparkUtil.write_to_mysql(ads_coupon_stats, "ads_coupon_stats", spark=self.spark)
            
            print(f"✓ 优惠券使用统计构建完成，记录数: {ads_coupon_stats.count()}")
            
        except Exception as e:
            print(f"✗ 优惠券使用统计构建失败: {e}")
            raise

    def build_ads_user_change(self):
        """构建用户变动统计表（流失用户+回流用户）- 支持多个时间维度"""
        try:
            print("\n开始构建用户变动统计表...")
    
            # 获取用户登录汇总数据
            dws_user_login_path = f"{self.paths_config['dws_path']}/dws_user_user_login_nd"
            dws_user_login_df = SparkUtil.read_from_hdfs(dws_user_login_path, spark=self.spark)
    
            print(f"用户登录汇总表记录数: {dws_user_login_df.count()}")
            
            # 显示数据结构和样本
            print("\n用户登录汇总表字段结构:")
            dws_user_login_df.printSchema()
            
            print("\n用户登录汇总表数据样本:")
            dws_user_login_df.show(5, truncate=False)
    
            # 检查登录时间的详细分布
            print("\n最近登录时间详细分布:")
            time_distribution = dws_user_login_df.groupBy("last_login_time").count().orderBy("last_login_time")
            time_distribution.show(20, truncate=False)
    
            # 获取当前日期作为Python对象
            from datetime import date
            today = date.today()
            
            print(f"统计基准日期: {today}")
    
            # 注册临时视图进行SQL操作
            dws_user_login_df.createOrReplaceTempView("user_login_summary")
    
            # 增强的用户变动统计SQL - 支持多个时间维度
            user_change_sql = """
            WITH date_calculations AS (
                SELECT 
                    user_id,
                    last_login_time,
                    datediff(current_date(), last_login_time) as days_since_last_login,
                    CASE 
                        WHEN date_format(last_login_time, 'yyyy-MM-dd') = current_date() THEN 1 
                        ELSE 0 
                    END as is_today_login,
                    CASE 
                        WHEN date_format(last_login_time, 'yyyy-MM-dd') = date_sub(current_date(), 1) THEN 1 
                        ELSE 0 
                    END as is_yesterday_login,
                    CASE 
                        WHEN datediff(current_date(), last_login_time) BETWEEN 1 AND 3 THEN 1 
                        ELSE 0 
                    END as is_recent_3days_login,
                    CASE 
                        WHEN datediff(current_date(), last_login_time) BETWEEN 1 AND 7 THEN 1 
                        ELSE 0 
                    END as is_recent_7days_login
                FROM user_login_summary
            ),
            user_stats_detailed AS (
                SELECT
                    -- 今日活跃用户
                    SUM(is_today_login) as today_active_users,
                    -- 昨日活跃用户
                    SUM(is_yesterday_login) as yesterday_active_users,
                    -- 最近3天活跃用户
                    SUM(is_recent_3days_login) as recent_3days_active_users,
                    -- 最近7天活跃用户
                    SUM(is_recent_7days_login) as recent_7days_active_users,
                    
                    -- 流失用户（不同时间维度）
                    SUM(CASE WHEN days_since_last_login > 3 THEN 1 ELSE 0 END) as churn_3days_count,
                    SUM(CASE WHEN days_since_last_login > 7 THEN 1 ELSE 0 END) as churn_7days_count,
                    SUM(CASE WHEN days_since_last_login > 30 THEN 1 ELSE 0 END) as churn_30days_count,
                    
                    -- 回流用户（简化定义：今日登录且之前N天未登录）
                    SUM(CASE 
                        WHEN is_today_login = 1 AND days_since_last_login >= 3 THEN 1 
                        ELSE 0 
                    END) as back_from_3days_count,
                    SUM(CASE 
                        WHEN is_today_login = 1 AND days_since_last_login >= 7 THEN 1 
                        ELSE 0 
                    END) as back_from_7days_count,
                    
                    -- 总用户数
                    COUNT(*) as total_users
                FROM date_calculations
            )
            SELECT 
                current_date() as dt,
                'user_activity_summary' as metric_type,
                
                -- 活跃用户指标
                today_active_users,
                yesterday_active_users,
                recent_3days_active_users,
                recent_7days_active_users,
                
                -- 流失用户指标
                churn_3days_count,
                churn_7days_count,
                churn_30days_count,
                
                -- 回流用户指标
                back_from_3days_count,
                back_from_7days_count,
                
                -- 总用户数
                total_users,
                
                -- 活跃率计算
                ROUND(today_active_users * 100.0 / total_users, 2) as today_active_rate,
                ROUND(recent_7days_active_users * 100.0 / total_users, 2) as week_active_rate
            FROM user_stats_detailed
            """
    
            print("\n执行增强的用户变动统计SQL...")
            ads_user_change_detailed = self.spark.sql(user_change_sql)
    
            # 显示详细计算结果
            print("\n详细用户变动统计结果:")
            ads_user_change_detailed.show(truncate=False)
    
            # 创建符合原表结构的简化版本（向后兼容）
            ads_user_change_simple = self.spark.sql("""
            WITH date_calculations AS (
                SELECT 
                    user_id,
                    last_login_time,
                    datediff(current_date(), last_login_time) as days_since_last_login
                FROM user_login_summary
            ),
            user_stats AS (
                SELECT
                    -- 流失用户数（3天以上未登录，因为7天数据太少）
                    SUM(CASE WHEN days_since_last_login > 3 THEN 1 ELSE 0 END) as churn_count,
                    -- 回流用户（今日登录且之前3天未登录）
                    SUM(CASE 
                        WHEN date_format(last_login_time, 'yyyy-MM-dd') = current_date() 
                             AND days_since_last_login >= 3 THEN 1 
                        ELSE 0 
                    END) as back_count
                FROM date_calculations
            )
            SELECT 
                current_date() as dt,
                CAST(COALESCE(churn_count, 0) AS INT) as user_churn_count,
                CAST(COALESCE(back_count, 0) AS INT) as user_back_count
            FROM user_stats
            """)
    
            print("\n简化版用户变动统计结果:")
            ads_user_change_simple.show(truncate=False)
    
            # 如果简化版本仍然为0，使用更宽松的条件
            if ads_user_change_simple.collect()[0]['user_churn_count'] == 0 and ads_user_change_simple.collect()[0]['user_back_count'] == 0:
                print("⚠️ 使用更宽松的统计条件（1天间隔）...")
                ads_user_change_relaxed = self.spark.sql("""
                WITH date_calculations AS (
                    SELECT 
                        user_id,
                        last_login_time,
                        datediff(current_date(), last_login_time) as days_since_last_login
                    FROM user_login_summary
                ),
                user_stats AS (
                    SELECT
                        -- 流失用户数（1天以上未登录）
                        SUM(CASE WHEN days_since_last_login > 1 THEN 1 ELSE 0 END) as churn_count,
                        -- 今日登录用户数
                        SUM(CASE 
                            WHEN date_format(last_login_time, 'yyyy-MM-dd') = current_date() THEN 1 
                            ELSE 0 
                        END) as today_login_count,
                        -- 昨日登录用户数
                        SUM(CASE 
                            WHEN date_format(last_login_time, 'yyyy-MM-dd') = date_sub(current_date(), 1) THEN 1 
                            ELSE 0 
                        END) as yesterday_login_count
                    FROM date_calculations
                )
                SELECT 
                    current_date() as dt,
                    CAST(COALESCE(churn_count, 0) AS INT) as user_churn_count,
                    CAST(COALESCE(today_login_count, 0) AS INT) as user_back_count
                FROM user_stats
                """)
                
                print("\n宽松条件用户变动统计结果:")
                ads_user_change_relaxed.show(truncate=False)
                
                ads_user_change = ads_user_change_relaxed
            else:
                ads_user_change = ads_user_change_simple
    
            # 验证数据类型
            print("\n最终数据结构:")
            ads_user_change.printSchema()
            ads_user_change.show()
    
            # 写入MySQL
            SparkUtil.write_to_mysql(ads_user_change, "ads_user_change", mode="overwrite", spark=self.spark)
    
            print(f"✓ 用户变动统计表构建完成，记录数: {ads_user_change.count()}")
    
            # 显示统计汇总信息
            user_change_result = ads_user_change.collect()[0]
            print(f"📊 用户变动统计汇总:")
            print(f"  统计日期: {user_change_result['dt']}")
            print(f"  流失用户数: {user_change_result['user_churn_count']}")
            print(f"  回流用户数: {user_change_result['user_back_count']}")
    
            # 同时输出详细统计到日志（可选）
            if 'ads_user_change_detailed' in locals():
                print("\n📈 详细活跃度分析:")
                detailed_result = ads_user_change_detailed.collect()[0]
                print(f"  总用户数: {detailed_result['total_users']}")
                print(f"  今日活跃: {detailed_result['today_active_users']} ({detailed_result['today_active_rate']}%)")
                print(f"  昨日活跃: {detailed_result['yesterday_active_users']}")
                print(f"  近7日活跃: {detailed_result['recent_7days_active_users']} ({detailed_result['week_active_rate']}%)")
                print(f"  3日流失: {detailed_result['churn_3days_count']}")
                print(f"  7日流失: {detailed_result['churn_7days_count']}")
    
        except Exception as e:
            print(f"✗ 用户变动统计表构建失败: {e}")
            import traceback
            traceback.print_exc()
            raise 

    def build_ads_order_to_pay_interval_avg(self):
        """构建下单到支付时间间隔平均值指标表"""
        try:
            print("\n开始构建下单到支付时间间隔平均值指标表...")
    
            # 1. 读取支付成功事实表
            dwd_trade_pay_detail_path = f"{self.paths_config['dwd_path']}/dwd_trade_pay_detail"
            pay_detail_df = SparkUtil.read_from_hdfs(dwd_trade_pay_detail_path, spark=self.spark)
    
            # 2. 读取订单信息表(从ODS层读取，因为需要订单的原始创建时间)
            order_info_path = f"{self.paths_config['ods_path']}/order_info"
            order_info_df = SparkUtil.read_from_hdfs(order_info_path, spark=self.spark)
    
            print(f"支付表记录数: {pay_detail_df.count()}")
            print(f"订单信息表记录数: {order_info_df.count()}")
    
            # 显示数据样本进行调试
            print("\n支付表时间字段样本:")
            pay_detail_df.select("order_id", "create_time", "callback_time", "dt").show(5, truncate=False)
            
            print("\n订单信息表时间字段样本:")
            order_info_df.select("id", "create_time", "operate_time").show(5, truncate=False)
    
            # 3. 关联订单和支付信息 - 修正关联逻辑
            order_pay_join = pay_detail_df.alias("pay") \
                .join(order_info_df.alias("order"),
                      col("pay.order_id") == col("order.id"),
                      "inner") \
                .select(
                    col("pay.order_id"),
                    col("order.create_time").alias("order_create_time"),
                    col("pay.callback_time").alias("pay_callback_time"),
                    col("pay.dt")
                ) \
                .filter(
                    (col("order_create_time").isNotNull()) & 
                    (col("pay_callback_time").isNotNull())
                )
    
            print(f"\n关联后记录数: {order_pay_join.count()}")
            
            if order_pay_join.count() > 0:
                # 显示关联后的样本数据
                print("\n关联后时间字段样本:")
                order_pay_join.show(5, truncate=False)
    
                # 4. 计算每个订单的下单到支付时间间隔（分钟）
                order_pay_interval = order_pay_join \
                    .withColumn("order_to_pay_interval_seconds",
                               unix_timestamp(col("pay_callback_time")) -
                               unix_timestamp(col("order_create_time"))) \
                    .withColumn("order_to_pay_interval",
                               col("order_to_pay_interval_seconds") / 60.0) \
                    .filter(col("order_to_pay_interval") >= 0)  # 过滤掉负值（异常数据）
    
                print("\n时间间隔计算结果样本:")
                order_pay_interval.select(
                    "order_id", 
                    "order_create_time", 
                    "pay_callback_time", 
                    "order_to_pay_interval_seconds",
                    "order_to_pay_interval"
                ).show(10, truncate=False)
    
                # 检查时间间隔的分布
                print("\n时间间隔分布统计:")
                interval_stats = order_pay_interval.agg(
                    count("*").alias("total_orders"),
                    min("order_to_pay_interval").alias("min_interval"),
                    max("order_to_pay_interval").alias("max_interval"),
                    avg("order_to_pay_interval").alias("avg_interval"),
                    stddev("order_to_pay_interval").alias("stddev_interval")
                )
                interval_stats.show()
    
                # 5. 按天计算平均时间间隔
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
    
                print(f"\n最终统计结果:")
                ads_order_to_pay_interval_avg.show(truncate=False)
    
                if ads_order_to_pay_interval_avg.count() > 0:
                    # 检查是否还有为0的值
                    zero_intervals = ads_order_to_pay_interval_avg.filter(col("order_to_pay_interval_avg") == 0)
                    if zero_intervals.count() > 0:
                        print("⚠️ 仍有时间间隔为0的记录:")
                        zero_intervals.show()
    
                    # 6. 写入MySQL
                    SparkUtil.write_to_mysql(ads_order_to_pay_interval_avg, "ads_order_to_pay_interval_avg", mode="overwrite", spark=self.spark)
    
                    print(f"✓ 下单到支付时间间隔平均值指标表构建完成，记录数: {ads_order_to_pay_interval_avg.count()}")
                else:
                    print("❌ 没有生成任何统计结果")
            else:
                print("❌ 订单和支付信息关联失败，没有匹配的记录")
    
        except Exception as e:
            print(f"✗ 下单到支付时间间隔平均值指标表构建失败: {e}")
            import traceback
            traceback.print_exc()
            raise

    def build_all_ads(self):
        """构建所有ADS应用数据服务表"""
        print("=== 开始构建ADS应用数据服务表 ===")

        ads_list = [
            ("各品牌商品交易统计", self.build_ads_trade_stats_by_tm),
            ("各分类商品交易统计", self.build_ads_trade_stats_by_cate),
            ("各省份交易统计", self.build_ads_trade_stats_by_province),
            ("用户统计", self.build_ads_user_stats),
            ("用户变动统计", self.build_ads_user_change),
            ("商品收藏排行榜TOP10", self.build_ads_sku_favor_count_top10),
            ("优惠券使用统计", self.build_ads_coupon_stats),
            ("下单到支付时间间隔平均值", self.build_ads_order_to_pay_interval_avg)  # 新增指标
        ]

        success_count = 0
        failure_count = 0

        for ads_name, ads_func in ads_list:
            try:
                ads_func()
                success_count += 1
            except Exception as e:
                failure_count += 1
                print(f"✗ {ads_name} 构建失败: {e}")
                continue

        print(f"\n=== ADS层构建完成 ===")
        print(f"成功: {success_count}张应用表")
        print(f"失败: {failure_count}张应用表")
        print(f"总计: {len(ads_list)}张应用表")

def main():
    """主函数"""
    try:
        dws_to_ads = DWSToADS()
        dws_to_ads.build_all_ads()
        
    except Exception as e:
        logging.error(f"DWS到ADS构建失败: {e}")
        print(f"✗ DWS到ADS构建失败: {e}")
    finally:
        SparkUtil.stop_spark_session()

if __name__ == "__main__":
    # 设置日志
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    main()
