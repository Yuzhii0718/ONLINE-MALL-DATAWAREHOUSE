"""
Hive ADS - 订单相关指标
包含：
- 品牌交易统计
- 品类交易统计
- 地区交易统计
"""
import logging
from config.config_manager import ConfigManager


class HiveADSOrders:
    def __init__(self, spark):
        self.spark = spark
        self.config_manager = ConfigManager()
        self.mysql_config = self.config_manager.get_mysql_config()
        self.hive_mysql_url = self.config_manager.get_hive_mysql_url()
    
    def build_ads_trade_stats_by_tm(self):
        """构建品牌交易统计"""
        try:
            print("\n开始构建品牌交易统计...")
            
            sql = """
            SELECT 
                ds.tm_id,
                ds.tm_name,
                SUM(dws.order_count) as order_count,
                SUM(dws.order_amount) as order_amount
            FROM dws_trade_sku_order_td dws
            INNER JOIN dim_sku ds ON dws.sku_id = ds.sku_id
            GROUP BY ds.tm_id, ds.tm_name
            ORDER BY order_amount DESC
            """
            
            result_df = self.spark.sql(sql)
            
            # 写入MySQL
            result_df.write \
                .format("jdbc") \
                .option("url", self.hive_mysql_url) \
                .option("dbtable", "ads_trade_stats_by_tm") \
                .option("user", self.mysql_config['username']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", self.mysql_config['driver']) \
                .mode("overwrite") \
                .save()
            
            count = result_df.count()
            print(f"✓ 品牌交易统计构建完成，记录数: {count}")
            return True
            
        except Exception as e:
            print(f"✗ 品牌交易统计构建失败: {e}")
            logging.error(f"构建ads_trade_stats_by_tm失败: {e}", exc_info=True)
            return False
    
    def build_ads_trade_stats_by_cate(self):
        """构建品类交易统计"""
        try:
            print("\n开始构建品类交易统计...")
            
            sql = """
            SELECT 
                ds.category1_id,
                ds.category1_name,
                ds.category2_id,
                ds.category2_name,
                ds.category3_id,
                ds.category3_name,
                SUM(dws.order_count) as order_count,
                SUM(dws.order_amount) as order_amount
            FROM dws_trade_sku_order_td dws
            INNER JOIN dim_sku ds ON dws.sku_id = ds.sku_id
            GROUP BY ds.category1_id, ds.category1_name, 
                     ds.category2_id, ds.category2_name,
                     ds.category3_id, ds.category3_name
            ORDER BY order_amount DESC
            """
            
            result_df = self.spark.sql(sql)
            
            # 写入MySQL
            result_df.write \
                .format("jdbc") \
                .option("url", self.hive_mysql_url) \
                .option("dbtable", "ads_trade_stats_by_cate") \
                .option("user", self.mysql_config['username']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", self.mysql_config['driver']) \
                .mode("overwrite") \
                .save()
            
            count = result_df.count()
            print(f"✓ 品类交易统计构建完成，记录数: {count}")
            return True
            
        except Exception as e:
            print(f"✗ 品类交易统计构建失败: {e}")
            logging.error(f"构建ads_trade_stats_by_cate失败: {e}", exc_info=True)
            return False
    
    def build_ads_trade_stats_by_province(self):
        """构建地区交易统计"""
        try:
            print("\n开始构建地区交易统计...")
            
            sql = """
            SELECT 
                dws.province_id,
                dp.province_name,
                dp.region_id,
                dp.region_name,
                SUM(dws.order_count) as order_count,
                SUM(dws.order_amount) as order_amount
            FROM dws_trade_province_order_td dws
            LEFT JOIN dim_province dp ON dws.province_id = dp.province_id
            GROUP BY dws.province_id, dp.province_name, dp.region_id, dp.region_name
            ORDER BY order_amount DESC
            """
            
            result_df = self.spark.sql(sql)
            
            # 写入MySQL
            result_df.write \
                .format("jdbc") \
                .option("url", self.hive_mysql_url) \
                .option("dbtable", "ads_trade_stats_by_province") \
                .option("user", self.mysql_config['username']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", self.mysql_config['driver']) \
                .mode("overwrite") \
                .save()
            
            count = result_df.count()
            print(f"✓ 地区交易统计构建完成，记录数: {count}")
            return True
            
        except Exception as e:
            print(f"✗ 地区交易统计构建失败: {e}")
            logging.error(f"构建ads_trade_stats_by_province失败: {e}", exc_info=True)
            return False

    def build_ads_top_sku_by_total_num(self):
        """历史被下单总件数最多的商品（SKU Top1，按件数）"""
        try:
            print("\n开始构建历史被下单总件数最多的商品（Top1）...")

            sql = """
            SELECT 
                sku_id,
                sku_name,
                order_num,
                order_count,
                order_amount
            FROM dws_trade_sku_order_td
            ORDER BY order_num DESC, sku_id
            LIMIT 1
            """

            result_df = self.spark.sql(sql)

            # 写入MySQL
            result_df.write \
                .format("jdbc") \
                .option("url", self.hive_mysql_url) \
                .option("dbtable", "ads_top_sku_by_total_num") \
                .option("user", self.mysql_config['username']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", self.mysql_config['driver']) \
                .mode("overwrite") \
                .save()

            count = result_df.count()
            print(f"✓ 历史被下单总件数最多的商品构建完成，记录数: {count}")
            return True

        except Exception as e:
            print(f"✗ 历史被下单总件数最多的商品构建失败: {e}")
            logging.error(f"构建ads_top_sku_by_total_num失败: {e}", exc_info=True)
            return False

    def build_ads_top_sku_by_province_7d(self):
        """最近一周每个省份下单件数最多的商品（各省SKU Top1，按最近7天件数）"""
        try:
            print("\n开始构建最近一周各省份下单件数最多的商品（Top1）...")

            sql = """
            WITH last7 AS (
                SELECT 
                    province_id,
                    sku_id,
                    sku_name,
                    SUM(sku_num) AS order_num_7d
                FROM dwd_trade_order_detail
                WHERE to_date(create_time) >= date_sub(current_date(), 6)
                GROUP BY province_id, sku_id, sku_name
            ), ranked AS (
                SELECT 
                    province_id,
                    sku_id,
                    sku_name,
                    order_num_7d,
                    ROW_NUMBER() OVER (PARTITION BY province_id ORDER BY order_num_7d DESC, sku_id) AS rn
                FROM last7
            )
            SELECT 
                r.province_id,
                dp.province_name,
                dp.region_id,
                dp.region_name,
                r.sku_id,
                r.sku_name,
                r.order_num_7d
            FROM ranked r
            LEFT JOIN dim_province dp ON r.province_id = dp.province_id
            WHERE r.rn = 1
            ORDER BY r.order_num_7d DESC
            """

            result_df = self.spark.sql(sql)

            # 写入MySQL
            result_df.write \
                .format("jdbc") \
                .option("url", self.hive_mysql_url) \
                .option("dbtable", "ads_top_sku_by_province_7d") \
                .option("user", self.mysql_config['username']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", self.mysql_config['driver']) \
                .mode("overwrite") \
                .save()

            count = result_df.count()
            print(f"✓ 最近一周各省份下单件数最多的商品构建完成，记录数: {count}")
            return True

        except Exception as e:
            print(f"✗ 最近一周各省份下单件数最多的商品构建失败: {e}")
            logging.error(f"构建ads_top_sku_by_province_7d失败: {e}", exc_info=True)
            return False

    def build_ads_top3_sku_by_province(self):
        """每个省份下单商品数量前三的商品（各省SKU Top3，按历史件数）"""
        try:
            print("\n开始构建每个省份下单商品数量前三的商品（Top3）...")

            sql = """
            WITH agg AS (
                SELECT 
                    province_id,
                    sku_id,
                    sku_name,
                    SUM(sku_num) AS order_num_total
                FROM dwd_trade_order_detail
                GROUP BY province_id, sku_id, sku_name
            ), ranked AS (
                SELECT 
                    province_id,
                    sku_id,
                    sku_name,
                    order_num_total,
                    DENSE_RANK() OVER (PARTITION BY province_id ORDER BY order_num_total DESC, sku_id) AS rk
                FROM agg
            )
            SELECT 
                r.province_id,
                dp.province_name,
                r.sku_id,
                r.sku_name,
                r.order_num_total,
                r.rk AS rank
            FROM ranked r
            LEFT JOIN dim_province dp ON r.province_id = dp.province_id
            WHERE r.rk <= 3
            ORDER BY r.province_id, r.rk, r.order_num_total DESC
            """

            result_df = self.spark.sql(sql)

            # 写入MySQL
            result_df.write \
                .format("jdbc") \
                .option("url", self.hive_mysql_url) \
                .option("dbtable", "ads_top3_sku_by_province") \
                .option("user", self.mysql_config['username']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", self.mysql_config['driver']) \
                .mode("overwrite") \
                .save()

            count = result_df.count()
            print(f"✓ 每个省份下单商品数量前三的商品构建完成，记录数: {count}")
            return True

        except Exception as e:
            print(f"✗ 每个省份下单商品数量前三的商品构建失败: {e}")
            logging.error(f"构建ads_top3_sku_by_province失败: {e}", exc_info=True)
            return False
