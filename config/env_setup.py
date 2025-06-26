"""
环境变量配置模块
设置PySpark运行所需的环境变量
"""
import os
import logging
from config.config_manager import ConfigManager

def setup_environment():
    """设置PySpark运行环境变量"""
    try:
        # 加载配置
        config_manager = ConfigManager()
        env_config = config_manager.get_environment_config()
        
        # 检查是否启用环境变量配置
        if not env_config.get('enable_env_config', True):
            print("✓ 环境变量配置已禁用，使用系统现有环境变量")
            print("当前系统环境变量:")
            
            # 显示当前系统环境变量
            system_vars = {
                'HADOOP_HOME': os.environ.get('HADOOP_HOME', '未设置'),
                'JAVA_HOME': os.environ.get('JAVA_HOME', '未设置'),
                'PYSPARK_PYTHON': os.environ.get('PYSPARK_PYTHON', '未设置'),
                'PYSPARK_DRIVER_PYTHON': os.environ.get('PYSPARK_DRIVER_PYTHON', '未设置')
            }
            
            for var_name, var_value in system_vars.items():
                print(f"  {var_name}: {var_value}")
            
            # 检查关键环境变量是否存在
            missing_vars = []
            if not os.environ.get('HADOOP_HOME'):
                missing_vars.append('HADOOP_HOME')
            if not os.environ.get('JAVA_HOME'):
                missing_vars.append('JAVA_HOME')
            
            if missing_vars:
                print(f"⚠️ 警告：以下关键环境变量未设置: {', '.join(missing_vars)}")
                print("请确保系统环境变量配置正确，否则可能影响程序运行")
            
            logging.info("使用系统环境变量，跳过环境变量设置")
            return
        
        # 启用配置文件中的环境变量设置
        print("✓ 环境变量配置已启用，应用配置文件中的设置")
        
        # 验证配置文件中的路径是否存在
        paths_to_check = {
            'HADOOP_HOME': env_config.get('hadoop_home', ''),
            'JAVA_HOME': env_config.get('java_home', ''),
        }
        
        missing_paths = []
        for var_name, path in paths_to_check.items():
            if path and not os.path.exists(path):
                missing_paths.append(f"{var_name}: {path}")
        
        if missing_paths:
            print("⚠️ 警告：以下配置路径不存在:")
            for missing_path in missing_paths:
                print(f"  {missing_path}")
        
        # Hadoop环境配置
        if env_config.get('hadoop_home'):
            os.environ['HADOOP_HOME'] = env_config['hadoop_home']
            os.environ["PATH"] += f";{env_config['hadoop_home']}\\bin"
            print(f"  ✓ HADOOP_HOME: {env_config['hadoop_home']}")
        
        # Java环境配置
        if env_config.get('java_home'):
            os.environ['JAVA_HOME'] = env_config['java_home']
            print(f"  ✓ JAVA_HOME: {env_config['java_home']}")
        
        # Python环境配置
        if env_config.get('pyspark_python'):
            os.environ['PYSPARK_PYTHON'] = env_config['pyspark_python']
            print(f"  ✓ PYSPARK_PYTHON: {env_config['pyspark_python']}")
        
        if env_config.get('pyspark_driver_python'):
            os.environ['PYSPARK_DRIVER_PYTHON'] = env_config['pyspark_driver_python']
            print(f"  ✓ PYSPARK_DRIVER_PYTHON: {env_config['pyspark_driver_python']}")
        
        logging.info("环境变量设置成功")
        print("✓ 环境变量配置完成")
        
    except Exception as e:
        logging.error(f"环境变量设置失败: {e}")
        print(f"✗ 环境变量设置失败: {e}")
        raise

def get_current_env_info():
    """获取当前环境变量信息（用于调试）"""
    env_vars = {
        'HADOOP_HOME': os.environ.get('HADOOP_HOME'),
        'JAVA_HOME': os.environ.get('JAVA_HOME'),
        'PYSPARK_PYTHON': os.environ.get('PYSPARK_PYTHON'),
        'PYSPARK_DRIVER_PYTHON': os.environ.get('PYSPARK_DRIVER_PYTHON'),
        'PATH': os.environ.get('PATH', '')[:200] + '...'  # 只显示PATH的前200个字符
    }
    
    print("\n🔍 当前环境变量状态:")
    for var_name, var_value in env_vars.items():
        status = "✓" if var_value else "✗"
        print(f"  {status} {var_name}: {var_value or '未设置'}")

if __name__ == "__main__":
    setup_environment()
    get_current_env_info()