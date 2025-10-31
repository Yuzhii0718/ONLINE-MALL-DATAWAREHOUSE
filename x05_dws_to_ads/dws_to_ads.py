"""
DWS到ADS层应用数据服务（重构版）
按照业务领域拆分为多个模块，便于维护与扩展。
"""
import logging
from tools.spark_util import SparkUtil
from config.config_manager import ConfigManager

# 引入分类后的ADS构建模块
from x05_dws_to_ads.ads.ads_orders import (
    build_trade_stats_by_tm,
    build_trade_stats_by_cate,
    build_trade_stats_by_province,
    build_order_to_pay_interval_avg,
)
from x05_dws_to_ads.ads.ads_users import (
    build_user_stats,
    build_user_change,
    build_user_retention,
)
from x05_dws_to_ads.ads.ads_favorites import build_sku_favor_count_top10
from x05_dws_to_ads.ads.ads_marketing import build_coupon_stats
from x05_dws_to_ads.ads.ads_product import build_sku_repurchase_rate


class DWSToADS:
    """ADS层编排器：负责创建Spark/配置，并按模块调用构建函数"""

    def __init__(self):
        self.spark = SparkUtil.get_spark_session("DWS-To-ADS")
        self.config_manager = ConfigManager()
        self.paths_config = self.config_manager.get_paths_config()

    def build_all_ads(self):
        """构建所有ADS应用数据服务表（按模块分类）"""
        print("=== 开始构建ADS应用数据服务表（分类调用） ===")

        tasks = [
            ("各品牌商品交易统计", build_trade_stats_by_tm),
            ("各分类商品交易统计", build_trade_stats_by_cate),
            ("各省份交易统计", build_trade_stats_by_province),
            ("用户统计", build_user_stats),
            ("用户变动统计", build_user_change),
            ("商品收藏排行榜TOP10", build_sku_favor_count_top10),
            ("优惠券使用统计", build_coupon_stats),
            ("下单到支付时间间隔平均值", build_order_to_pay_interval_avg),
            ("用户留存率指标", build_user_retention),
            ("商品复购率指标", build_sku_repurchase_rate),
        ]

        success_count = 0
        failure_count = 0

        for name, func in tasks:
            try:
                func(self.spark, self.paths_config)
                success_count += 1
            except Exception as e:
                failure_count += 1
                print(f"✗ {name} 构建失败: {e}")
                continue

        print("\n=== ADS层构建完成 ===")
        print(f"成功: {success_count} 张应用表")
        print(f"失败: {failure_count} 张应用表")
        print(f"总计: {len(tasks)} 张应用表")


def main():
    try:
        orchestrator = DWSToADS()
        orchestrator.build_all_ads()
    except Exception as e:
        logging.error(f"DWS到ADS构建失败: {e}")
        print(f"✗ DWS到ADS构建失败: {e}")
    finally:
        SparkUtil.stop_spark_session()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
