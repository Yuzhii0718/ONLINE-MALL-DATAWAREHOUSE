"""
MySQL到ODS层数据同步
将MySQL中的业务数据原封不动地导入到HDFS的ODS层
"""
import logging
from datetime import datetime
from tools.spark_util import SparkUtil
from config.config_manager import ConfigManager

class MySQLToODS:
    def __init__(self):
        self.spark = SparkUtil.get_spark_session("MySQL-To-ODS")
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
        """同步单个表到ODS层"""
        try:
            print(f"\n开始同步表: {table_name}")
            
            # 从MySQL读取数据
            df = SparkUtil.read_from_mysql(table_name, self.spark)
            
            # 写入HDFS ODS层
            ods_path = f"{self.paths_config['ods_path']}/{table_name}"
            SparkUtil.write_to_hdfs(df, ods_path, spark=self.spark)
            
            print(f"✓ 表 {table_name} 同步完成")
            
        except Exception as e:
            logging.error(f"同步表 {table_name} 失败: {e}")
            print(f"✗ 同步表 {table_name} 失败: {e}")
            raise
    
    def sync_all_tables(self):
        """同步所有表到ODS层"""
        print("=== 开始MySQL到ODS层数据同步 ===")
        print(f"目标路径: {self.paths_config['ods_path']}")
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
    
    def sync_incremental_tables(self, table_list):
        """增量同步指定表"""
        print(f"=== 开始增量同步 ===")
        print(f"同步表: {', '.join(table_list)}")
        
        for table_name in table_list:
            if table_name in self.tables:
                try:
                    self.sync_table(table_name)
                except Exception as e:
                    print(f"✗ 增量同步表 {table_name} 失败: {e}")
            else:
                print(f"⚠ 表 {table_name} 不在同步列表中")

def main():
    """主函数"""
    try:
        mysql_to_ods = MySQLToODS()
        mysql_to_ods.sync_all_tables()
        
    except Exception as e:
        logging.error(f"MySQL到ODS同步失败: {e}")
        print(f"✗ MySQL到ODS同步失败: {e}")
    finally:
        SparkUtil.stop_spark_session()

if __name__ == "__main__":
    # 设置日志
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    main()
