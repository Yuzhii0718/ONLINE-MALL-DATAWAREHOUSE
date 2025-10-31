"""
Hive模式: DWS到ADS层应用数据服务
使用HiveQL构建应用层指标并导出到MySQL
"""
import logging
from tools.spark_util import SparkUtil
from config.config_manager import ConfigManager
from hive.ads.hive_ads_orders import HiveADSOrders
from hive.ads.hive_ads_users import HiveADSUsers
from hive.ads.hive_ads_favorites import HiveADSFavorites


class HiveDWSToADS:
    def __init__(self):
        self.spark = SparkUtil.create_spark_session("Hive-DWS-To-ADS")
        self.config_manager = ConfigManager()
        
        # 初始化各个ADS指标类
        self.ads_orders = HiveADSOrders(self.spark)
        self.ads_users = HiveADSUsers(self.spark)
        self.ads_favorites = HiveADSFavorites(self.spark)
    
    def build_ads_trade_stats_by_tm(self):
        """构建品牌交易统计（委托给订单指标类）"""
        return self.ads_orders.build_ads_trade_stats_by_tm()
    
    def build_ads_trade_stats_by_cate(self):
        """构建品类交易统计（委托给订单指标类）"""
        return self.ads_orders.build_ads_trade_stats_by_cate()
    
    def build_ads_trade_stats_by_province(self):
        """构建地区交易统计（委托给订单指标类）"""
        return self.ads_orders.build_ads_trade_stats_by_province()
    
    def build_ads_user_stats(self):
        """构建用户统计（委托给用户指标类）"""
        return self.ads_users.build_ads_user_stats()
    
    def build_ads_sku_favor_count_top10(self):
        """构建商品收藏Top10（委托给收藏指标类）"""
        return self.ads_favorites.build_ads_sku_favor_count_top10()

    def build_ads_top_sku_by_total_num(self):
        """构建历史被下单总件数最多的商品（委托给订单指标类）"""
        return self.ads_orders.build_ads_top_sku_by_total_num()

    def build_ads_top_sku_by_province_7d(self):
        """构建最近一周各省份下单件数最多的商品（委托给订单指标类）"""
        return self.ads_orders.build_ads_top_sku_by_province_7d()

    def build_ads_top3_sku_by_province(self):
        """构建每个省份下单商品数量前三的商品（委托给订单指标类）"""
        return self.ads_orders.build_ads_top3_sku_by_province()
    
    def build_all_ads(self):
        """构建所有ADS应用指标"""
        print("=== 开始Hive DWS到ADS层构建 ===")
        print(f"目标MySQL数据库: {self.config_manager.get_hive_config()['exec_mysql_database']}")
        
        ads_tasks = [
            # 订单相关指标
            ("品牌交易统计", self.build_ads_trade_stats_by_tm),
            ("品类交易统计", self.build_ads_trade_stats_by_cate),
            ("地区交易统计", self.build_ads_trade_stats_by_province),
            ("历史被下单总件数最多的商品", self.build_ads_top_sku_by_total_num),
            ("最近一周各省份下单件数最多的商品", self.build_ads_top_sku_by_province_7d),
            ("各省下单商品数量Top3", self.build_ads_top3_sku_by_province),
            # 用户相关指标
            ("用户统计", self.build_ads_user_stats),
            # 收藏相关指标
            ("商品收藏Top10", self.build_ads_sku_favor_count_top10)
        ]
        
        success_count = 0
        for ads_name, build_func in ads_tasks:
            if build_func():
                success_count += 1
        
        print(f"\n=== ADS层构建完成 ===")
        print(f"成功: {success_count}/{len(ads_tasks)}")
        
        return success_count == len(ads_tasks)

