"""
GMALL数据仓库Hive模式主运行脚本
使用Hive进行数据存储和查询
"""
import logging
import os
import sys
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.env_setup import setup_environment
from config.config_manager import ConfigManager
from hive.hive_mysql_to_ods import HiveMySQLToODS
from hive.hive_ods_to_dim import HiveODSToDIM
from hive.hive_ods_to_dwd import HiveODSToDWD
from hive.hive_dwd_to_dws import HiveDWDToDWS
from hive.hive_dws_to_ads import HiveDWSToADS

def run_hive_pipeline():
    """运行基于Hive的数据仓库流水线"""
    start_time = datetime.now()
    
    print("🚀 GMALL数据仓库流水线开始执行 (Hive模式)")
    print(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 执行步骤
    steps = [
        ("MySQL到ODS (Hive)", run_mysql_to_ods_hive),
        ("ODS到DIM (Hive)", run_ods_to_dim_hive),
        ("ODS到DWD (Hive)", run_ods_to_dwd_hive),
        ("DWD到DWS (Hive)", run_dwd_to_dws_hive),
        ("DWS到ADS (Hive)", run_dws_to_ads_hive)
    ]
    
    success_count = 0
    failed_steps = []
    
    for step_name, step_func in steps:
        try:
            if step_func():
                success_count += 1
                print(f"✅ {step_name} - 成功")
            else:
                failed_steps.append(step_name)
                print(f"❌ {step_name} - 失败")
        except Exception as e:
            failed_steps.append(step_name)
            print(f"❌ {step_name} - 异常: {e}")
            logging.error(f"{step_name} 执行异常: {e}", exc_info=True)
    
    # 总结报告
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "="*60)
    print("🎯 GMALL数据仓库流水线执行完成 (Hive模式)")
    print("="*60)
    print(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"总耗时: {duration}")
    print(f"成功步骤: {success_count}/{len(steps)}")
    
    if failed_steps:
        print(f"失败步骤: {', '.join(failed_steps)}")
        return False
    
    return True
def run_mysql_to_ods_hive():
    """使用Hive运行MySQL到ODS数据同步"""
    try:
        print("\n" + "="*50)
        print("步骤1: MySQL到ODS层数据同步 (Hive)")
        print("="*50)
        
        mysql_to_ods = HiveMySQLToODS()
        success = mysql_to_ods.sync_all_tables()
        
        if success:
            print("✓ MySQL到ODS层数据同步完成 (Hive)")
        return success
        
    except Exception as e:
        print(f"✗ MySQL到ODS层数据同步失败: {e}")
        logging.error(f"MySQL到ODS (Hive) 失败: {e}", exc_info=True)
        return False

def run_ods_to_dim_hive():
    """使用Hive运行ODS到DIM维度表构建"""
    try:
        print("\n" + "="*50)
        print("步骤2: ODS到DIM维度表构建 (Hive)")
        print("="*50)
        
        ods_to_dim = HiveODSToDIM()
        success = ods_to_dim.build_all_dims()
        
        if success:
            print("✓ ODS到DIM维度表构建完成 (Hive)")
        return success
        
    except Exception as e:
        print(f"✗ ODS到DIM维度表构建失败: {e}")
        logging.error(f"ODS到DIM (Hive) 失败: {e}", exc_info=True)
        return False

def run_ods_to_dwd_hive():
    """使用Hive运行ODS到DWD事实表构建"""
    try:
        print("\n" + "="*50)
        print("步骤3: ODS到DWD事实表构建 (Hive)")
        print("="*50)
        
        ods_to_dwd = HiveODSToDWD()
        success = ods_to_dwd.build_all_dwds()
        
        if success:
            print("✓ ODS到DWD事实表构建完成 (Hive)")
        return success
        
    except Exception as e:
        print(f"✗ ODS到DWD事实表构建失败: {e}")
        logging.error(f"ODS到DWD (Hive) 失败: {e}", exc_info=True)
        return False

def run_dwd_to_dws_hive():
    """使用Hive运行DWD到DWS轻度聚合"""
    try:
        print("\n" + "="*50)
        print("步骤4: DWD到DWS轻度聚合 (Hive)")
        print("="*50)
        
        dwd_to_dws = HiveDWDToDWS()
        success = dwd_to_dws.build_all_dws()
        
        if success:
            print("✓ DWD到DWS轻度聚合完成 (Hive)")
        return success
        
    except Exception as e:
        print(f"✗ DWD到DWS轻度聚合失败: {e}")
        logging.error(f"DWD到DWS (Hive) 失败: {e}", exc_info=True)
        return False

def run_dws_to_ads_hive():
    """使用Hive运行DWS到ADS应用数据服务"""
    try:
        print("\n" + "="*50)
        print("步骤5: DWS到ADS应用数据服务 (Hive)")
        print("="*50)
        
        dws_to_ads = HiveDWSToADS()
        success = dws_to_ads.build_all_ads()
        
        if success:
            print("✓ DWS到ADS应用数据服务完成 (Hive)")
        return success
        
    except Exception as e:
        print(f"✗ DWS到ADS应用数据服务失败: {e}")
        logging.error(f"DWS到ADS (Hive) 失败: {e}", exc_info=True)
        return False

def run_single_step_hive(step_name):
    """运行单个Hive步骤"""
    step_map = {
        "mysql_to_ods": ("MySQL到ODS (Hive)", run_mysql_to_ods_hive),
        "ods_to_dim": ("ODS到DIM (Hive)", run_ods_to_dim_hive),
        "ods_to_dwd": ("ODS到DWD (Hive)", run_ods_to_dwd_hive),
        "dwd_to_dws": ("DWD到DWS (Hive)", run_dwd_to_dws_hive),
        "dws_to_ads": ("DWS到ADS (Hive)", run_dws_to_ads_hive)
    }
    
    if step_name not in step_map:
        print(f"❌ 未知步骤: {step_name}")
        print(f"可用步骤: {', '.join(step_map.keys())}")
        return False
    
    step_desc, step_func = step_map[step_name]
    print(f"🚀 执行单个步骤: {step_desc}")
    
    try:
        return step_func()
    except Exception as e:
        print(f"❌ 步骤 {step_desc} 执行失败: {e}")
        logging.error(f"{step_desc} 执行失败: {e}", exc_info=True)
        return False

def main():
    """Hive模式主函数"""
    try:
        # 设置环境变量
        setup_environment()
        
        # 检查配置
        config_manager = ConfigManager()
        hive_config = config_manager.get_hive_config()
        
        if not hive_config.get('enable_hive_support'):
            print("⚠️ Hive支持未启用，请在config.ini中设置 enable_hive_support = true")
            return False
        
        print("✅ Hive模式已启用")
        print("✅ 配置检查完成")
        
        # 根据命令行参数决定执行模式
        if len(sys.argv) > 1:
            step_name = sys.argv[1]
            success = run_single_step_hive(step_name)
        else:
            success = run_hive_pipeline()
        
        if success:
            print("\n🎉 程序执行成功！")
            return True
        else:
            print("\n❌ 程序执行失败！")
            return False
            
    except Exception as e:
        print(f"❌ 程序执行异常: {e}")
        logging.error(f"程序执行异常: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
