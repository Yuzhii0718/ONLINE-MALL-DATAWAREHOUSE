"""
Hive模式: ODS到DIM层维度表构建
使用HiveQL从ODS层构建各维度表
"""
import logging
from tools.spark_util import SparkUtil
from config.config_manager import ConfigManager
from datetime import datetime, timedelta

class HiveODSToDIM:
    def __init__(self):
        self.spark = SparkUtil.create_spark_session("Hive-ODS-To-DIM")
        self.config_manager = ConfigManager()
        self.paths_config = self.config_manager.get_paths_config()
    
    def build_dim_date(self):
        """构建日期维度表"""
        try:
            print("\n开始构建日期维度表...")
            
            # 生成2020-2030年的日期维度数据
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
            
            # 创建DataFrame并写入Hive（使用外部路径，避免默认仓库冲突）
            date_df = self.spark.createDataFrame(dates)
            target_path = f"{self.paths_config['dim_path']}/dim_date"
            
            self.spark.sql("DROP TABLE IF EXISTS dim_date")
            date_df.write \
                .mode("overwrite") \
                .format("parquet") \
                .option("path", target_path) \
                .saveAsTable("dim_date")
            
            count = self.spark.table("dim_date").count()
            print(f"✓ 日期维度表构建完成，记录数: {count}")
            return True
            
        except Exception as e:
            print(f"✗ 日期维度表构建失败: {e}")
            logging.error(f"构建dim_date失败: {e}", exc_info=True)
            return False
    
    def build_dim_user(self):
        """构建用户维度表"""
        try:
            print("\n开始构建用户维度表...")
            
            select_sql = """
            SELECT 
                id as user_id,
                login_name,
                nick_name,
                name as real_name,
                phone_num,
                email,
                user_level,
                birthday,
                gender,
                create_time,
                operate_time,
                status,
                CAST(NULL AS BIGINT) as province_id,
                CAST(NULL AS STRING) as province_name,
                CAST(NULL AS BIGINT) as region_id,
                CAST(NULL AS STRING) as region_name
            FROM ods_user_info
            """
            
            dim_user_df = self.spark.sql(select_sql)
            target_path = f"{self.paths_config['dim_path']}/dim_user"
            
            self.spark.sql("DROP TABLE IF EXISTS dim_user")
            dim_user_df.write \
                .mode("overwrite") \
                .format("parquet") \
                .option("path", target_path) \
                .saveAsTable("dim_user")
            
            count = self.spark.table("dim_user").count()
            print(f"✓ 用户维度表构建完成，记录数: {count}")
            return True
            
        except Exception as e:
            print(f"✗ 用户维度表构建失败: {e}")
            logging.error(f"构建dim_user失败: {e}", exc_info=True)
            return False
    
    def build_dim_sku(self):
        """构建商品SKU维度表"""
        try:
            print("\n开始构建商品SKU维度表...")
            
            select_sql = """
            SELECT 
                si.id as sku_id,
                si.spu_id,
                si.price,
                si.sku_name,
                si.sku_desc,
                si.weight,
                si.tm_id,
                bt.tm_name,
                si.category3_id,
                bc3.name as category3_name,
                bc3.category2_id,
                bc2.name as category2_name,
                bc2.category1_id,
                bc1.name as category1_name,
                si.create_time
            FROM ods_sku_info si
            LEFT JOIN ods_base_trademark bt ON si.tm_id = bt.id
            LEFT JOIN ods_base_category3 bc3 ON si.category3_id = bc3.id
            LEFT JOIN ods_base_category2 bc2 ON bc3.category2_id = bc2.id
            LEFT JOIN ods_base_category1 bc1 ON bc2.category1_id = bc1.id
            """
            
            dim_sku_df = self.spark.sql(select_sql)
            target_path = f"{self.paths_config['dim_path']}/dim_sku"
            
            self.spark.sql("DROP TABLE IF EXISTS dim_sku")
            dim_sku_df.write \
                .mode("overwrite") \
                .format("parquet") \
                .option("path", target_path) \
                .saveAsTable("dim_sku")
            
            count = self.spark.table("dim_sku").count()
            print(f"✓ 商品SKU维度表构建完成，记录数: {count}")
            return True
            
        except Exception as e:
            print(f"✗ 商品SKU维度表构建失败: {e}")
            logging.error(f"构建dim_sku失败: {e}", exc_info=True)
            return False
    
    def build_dim_province(self):
        """构建地区维度表"""
        try:
            print("\n开始构建地区维度表...")
            
            select_sql = """
            SELECT 
                bp.id as province_id,
                bp.name as province_name,
                bp.area_code,
                bp.iso_code,
                bp.region_id,
                br.region_name
            FROM ods_base_province bp
            LEFT JOIN ods_base_region br ON bp.region_id = br.id
            """
            
            dim_province_df = self.spark.sql(select_sql)
            target_path = f"{self.paths_config['dim_path']}/dim_province"
            
            self.spark.sql("DROP TABLE IF EXISTS dim_province")
            dim_province_df.write \
                .mode("overwrite") \
                .format("parquet") \
                .option("path", target_path) \
                .saveAsTable("dim_province")
            
            count = self.spark.table("dim_province").count()
            print(f"✓ 地区维度表构建完成，记录数: {count}")
            return True
            
        except Exception as e:
            print(f"✗ 地区维度表构建失败: {e}")
            logging.error(f"构建dim_province失败: {e}", exc_info=True)
            return False
    
    def build_all_dims(self):
        """构建所有维度表"""
        print("=== 开始Hive ODS到DIM层构建 ===")
        
        dims = [
            ("日期维度", self.build_dim_date),
            ("用户维度", self.build_dim_user),
            ("商品SKU维度", self.build_dim_sku),
            ("地区维度", self.build_dim_province)
        ]
        
        success_count = 0
        for dim_name, build_func in dims:
            if build_func():
                success_count += 1
        
        print(f"\n=== DIM层构建完成 ===")
        print(f"成功: {success_count}/{len(dims)}")
        
        return success_count == len(dims)
