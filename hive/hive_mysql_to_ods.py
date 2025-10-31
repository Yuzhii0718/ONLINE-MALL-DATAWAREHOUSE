"""
Hive模式: MySQL到ODS层数据同步
使用Spark enableHiveSupport将MySQL数据导入Hive表
"""
import logging
from tools.spark_util import SparkUtil
from config.config_manager import ConfigManager

class HiveMySQLToODS:
    def __init__(self):
        # 使用enableHiveSupport创建Spark会话
        self.spark = SparkUtil.create_spark_session("Hive-MySQL-To-ODS")
        self.config_manager = ConfigManager()
        self.paths_config = self.config_manager.get_paths_config()
        
        # 需要同步的表列表
        self.tables = [
            'activity_info', 'activity_rule', 'base_category1', 'base_category2', 
            'base_category3', 'base_dic', 'base_province', 'base_region', 
            'base_trademark', 'cart_info', 'coupon_info', 'comment_info',
            'coupon_use', 'favor_info', 'order_detail', 'order_detail_activity',
            'order_detail_coupon', 'order_info', 'order_refund_info', 'payment_info',
            'refund_payment', 'sku_attr_value', 'sku_info', 'sku_sale_attr_value',
            'spu_info', 'spu_sale_attr', 'spu_sale_attr_value', 'user_info'
        ]
    
    def sync_table(self, table_name):
        """同步单个表到Hive ODS层"""
        try:
            print(f"\n开始同步表: {table_name}")
            
            # 从MySQL读取数据
            df = SparkUtil.read_from_mysql(table_name, self.spark)
            
            # 使用外部路径 + saveAsTable 避免默认仓库冲突，可重复执行覆盖
            hive_table = f"ods_{table_name}"
            target_path = f"{self.paths_config['ods_path']}/{hive_table}"

            # 清理元数据，防止历史 schema 残留
            self.spark.sql(f"DROP TABLE IF EXISTS {hive_table}")

            # 覆盖写入到指定路径，并注册为数据源表
            df.write \
                .mode("overwrite") \
                .format("parquet") \
                .option("path", target_path) \
                .saveAsTable(hive_table)

            record_count = self.spark.table(hive_table).count()
            print(f"✓ 表 {table_name} 同步到Hive完成，记录数: {record_count}")
            
        except Exception as e:
            logging.error(f"同步表 {table_name} 到Hive失败: {e}")
            print(f"✗ 同步表 {table_name} 到Hive失败: {e}")
            raise
    
    def sync_all_tables(self):
        """同步所有表到Hive ODS层"""
        print("=== 开始MySQL到Hive ODS层数据同步 ===")
        print(f"同步表数量: {len(self.tables)}")
        
        success_count = 0
        failure_count = 0
        
        for table_name in self.tables:
            try:
                self.sync_table(table_name)
                success_count += 1
            except Exception as e:
                failure_count += 1
                print(f"✗ 表 {table_name} 同步失败: {e}")
                continue
        
        print(f"\n=== 同步完成 ===")
        print(f"成功: {success_count}张表")
        print(f"失败: {failure_count}张表")
        print(f"总计: {len(self.tables)}张表")
        
        return failure_count == 0
