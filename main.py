"""
GMALL数据仓库主运行脚本
按顺序执行各层数据处理
"""
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import sys
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.env_setup import setup_environment
from config.config_manager import ConfigManager
from tools.spark_util import SparkUtil

def run_mysql_to_ods():
    """运行MySQL到ODS数据同步"""
    try:
        print("\n" + "="*50)
        print("步骤1: MySQL到ODS层数据同步")
        print("="*50)
        
        from x01_mysql_to_ods.mysql_to_ods import MySQLToODS
        mysql_to_ods = MySQLToODS()
        mysql_to_ods.sync_all_tables()
        
        print("✓ MySQL到ODS层数据同步完成")
        return True
        
    except Exception as e:
        print(f"✗ MySQL到ODS层数据同步失败: {e}")
        return False

def run_ods_to_dim():
    """运行ODS到DIM维度表构建"""
    try:
        print("\n" + "="*50)
        print("步骤2: ODS到DIM维度表构建")
        print("="*50)
        
        from x02_ods_to_dim.ods_to_dim import ODSToDIM
        ods_to_dim = ODSToDIM()
        ods_to_dim.build_all_dims()
        
        print("✓ ODS到DIM维度表构建完成")
        return True
        
    except Exception as e:
        print(f"✗ ODS到DIM维度表构建失败: {e}")
        return False

def run_ods_to_dwd():
    """运行ODS到DWD事实表构建"""
    try:
        print("\n" + "="*50)
        print("步骤3: ODS到DWD事实表构建")
        print("="*50)
        
        from x03_ods_to_dwd.ods_to_dwd import ODSToDWD
        ods_to_dwd = ODSToDWD()
        ods_to_dwd.build_all_dwds()
        
        print("✓ ODS到DWD事实表构建完成")
        return True
        
    except Exception as e:
        print(f"✗ ODS到DWD事实表构建失败: {e}")
        return False

def run_dwd_to_dws():
    """运行DWD到DWS轻度聚合"""
    try:
        print("\n" + "="*50)
        print("步骤4: DWD到DWS轻度聚合")
        print("="*50)
        
        from x04_dwd_to_dws.dwd_to_dws import DWDToDWS
        dwd_to_dws = DWDToDWS()
        dwd_to_dws.build_all_dws()
        
        print("✓ DWD到DWS轻度聚合完成")
        return True
        
    except Exception as e:
        print(f"✗ DWD到DWS轻度聚合失败: {e}")
        return False

def run_dws_to_ads():
    """运行DWS到ADS应用数据服务"""
    try:
        print("\n" + "="*50)
        print("步骤5: DWS到ADS应用数据服务")
        print("="*50)
        
        from x05_dws_to_ads.dws_to_ads import DWSToADS
        dws_to_ads = DWSToADS()
        dws_to_ads.build_all_ads()
        
        print("✓ DWS到ADS应用数据服务完成")
        return True
        
    except Exception as e:
        print(f"✗ DWS到ADS应用数据服务失败: {e}")
        return False

def run_full_pipeline():
    """运行完整数据仓库流水线"""
    start_time = datetime.now()
    
    print("🚀 GMALL数据仓库流水线开始执行")
    print(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 执行步骤
    steps = [
        ("MySQL到ODS", run_mysql_to_ods),
        ("ODS到DIM", run_ods_to_dim),
        ("ODS到DWD", run_ods_to_dwd),
        ("DWD到DWS", run_dwd_to_dws),
        ("DWS到ADS", run_dws_to_ads)
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
    
    # 总结报告
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "="*60)
    print("🎯 GMALL数据仓库流水线执行完成")
    print("="*60)
    print(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"总耗时: {duration}")
    print(f"成功步骤: {success_count}/{len(steps)}")
    
    if failed_steps:
        print(f"失败步骤: {', '.join(failed_steps)}")
    else:
        print("🎉 所有步骤执行成功！")
    
    return len(failed_steps) == 0

def run_single_step(step_name):
    """运行单个步骤"""
    step_map = {
        "mysql_to_ods": ("MySQL到ODS", run_mysql_to_ods),
        "ods_to_dim": ("ODS到DIM", run_ods_to_dim),
        "ods_to_dwd": ("ODS到DWD", run_ods_to_dwd),
        "dwd_to_dws": ("DWD到DWS", run_dwd_to_dws),
        "dws_to_ads": ("DWS到ADS", run_dws_to_ads)
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
        return False

def setup_logging():
    """设置按天轮转的日志配置"""
    # 创建日志目录
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # 日志文件路径
    log_file = os.path.join(log_dir, 'gmall_datawarehouse.log')
    
    # 创建按天轮转的文件处理器
    file_handler = TimedRotatingFileHandler(
        filename=log_file,
        when='midnight',  # 每天午夜轮转
        interval=1,       # 间隔1天
        backupCount=30,   # 保留30天的日志
        encoding='utf-8'
    )
    
    # 设置日志格式
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # 配置根日志记录器
    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, console_handler]
    )

def main():
    """主函数"""

    # 设置日志
    setup_logging()
    
    try:
        # 设置环境变量
        setup_environment()
        
        # 检查配置
        config_manager = ConfigManager()
        print("✅ 配置检查完成")
        
        # 根据命令行参数决定执行模式
        # 当参数：`step_name` 时，执行单个步骤，比如：`python main.py mysql_to_ods`
        if len(sys.argv) > 1:
            step_name = sys.argv[1]
            success = run_single_step(step_name)
        else:
            success = run_full_pipeline()
        
        if success:
            print("\n🎉 程序执行成功！")
            sys.exit(0)
        else:
            print("\n❌ 程序执行失败！")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ 程序执行异常: {e}")
        logging.error(f"程序执行异常: {e}", exc_info=True)
        sys.exit(1)
    finally:
        SparkUtil.stop_spark_session()

if __name__ == "__main__":
    main()
