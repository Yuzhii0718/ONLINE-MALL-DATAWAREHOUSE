"""
订单数据生成脚本
生成丰富、真实、多样化的订单数据用于测试ADS指标
- 历史被下单总件数最多的商品
- 最近一周每个省份下单件数最多的商品
- 每个省份下单商品数量前三的商品
"""
import mysql.connector
from datetime import datetime, timedelta
import random
import sys
import os

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config_manager import ConfigManager


class OrderDataGenerator:
    def __init__(self):
        self.config_manager = ConfigManager()
        self.mysql_config = self.config_manager.get_mysql_config()
        self.conn = None
        self.cursor = None
        
        # 省份ID列表（基于gmall.sql中的数据）
        self.province_ids = list(range(1, 35))  # 34个省份
        
        # SKU信息：根据实际gmall.sql中的sku_info表
        # 这里定义一批热门商品和普通商品的组合
        self.hot_skus = [
            (1, '小米10 青春版 5G 6GB+64GB', 1999.00),
            (2, '小米10 青春版 5G 6GB+128GB', 2299.00),
            (3, '小米10 青春版 5G 8GB+128GB', 2499.00),
            (13, 'ThinkPad X1 Carbon i5-10210U 8G 512G', 8999.00),
            (14, 'ThinkPad X1 Carbon i7-10510U 16G 512G', 11999.00),
            (15, 'ThinkPad X1 Carbon i7-10510U 16G 1T', 13999.00),
            (17, 'TCL 65V8 65英寸 4K超高清', 3999.00),
            (18, 'TCL 75V8 75英寸 4K超高清', 5999.00),
        ]
        
        self.normal_skus = [
            (16, 'ThinkPad X1 Carbon i5-10210U 16G 512G', 9999.00),
            (19, 'TCL 85V8 85英寸 8K超高清', 12999.00),
            (20, 'Canon EOS R5 全画幅微单', 25999.00),
            (21, 'Canon EOS R6 全画幅微单', 15999.00),
            (22, 'Sony A7M4 全画幅微单', 18999.00),
            (23, 'Nikon Z9 全画幅微单', 35999.00),
            (26, 'CAREMiLLE 口红 #01 正红色', 199.00),
            (27, 'CAREMiLLE 口红 #02 豆沙色', 199.00),
            (28, 'CAREMiLLE 口红 #03 姨妈色', 199.00),
            (29, 'Dior 口红 #999', 299.00),
            (30, 'YSL 口红 #52', 329.00),
        ]
        
        # 用户ID范围（假设系统中有100个用户）
        self.user_ids = list(range(1, 101))
        
    def connect(self):
        """连接MySQL数据库"""
        try:
            self.conn = mysql.connector.connect(
                host=self.mysql_config['host'],
                port=int(self.mysql_config['port']),
                user=self.mysql_config['username'],
                password=self.mysql_config['password'],
                database=self.mysql_config['database']
            )
            self.cursor = self.conn.cursor()
            print(f"✓ 成功连接到MySQL数据库: {self.mysql_config['database']}")
        except Exception as e:
            print(f"✗ 数据库连接失败: {e}")
            raise
    
    def close(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✓ 数据库连接已关闭")
    
    def clear_existing_orders(self):
        """清空现有订单数据"""
        try:
            print("\n开始清空现有订单数据...")
            tables = [
                'order_detail_coupon',
                'order_detail_activity',
                'order_detail',
                'order_status_log',
                'payment_info',
                'order_info'
            ]
            
            for table in tables:
                self.cursor.execute(f"DELETE FROM {table}")
                print(f"  - 清空表: {table}")
            
            self.conn.commit()
            print("✓ 现有订单数据清空完成")
        except Exception as e:
            self.conn.rollback()
            print(f"✗ 清空订单数据失败: {e}")
            raise
    
    def generate_historical_orders(self, days=90, orders_per_day_range=(50, 150)):
        """
        生成历史订单数据（用于验证历史总件数最多的商品）
        
        Args:
            days: 生成多少天的历史数据
            orders_per_day_range: 每天订单数量范围
        """
        print(f"\n开始生成历史订单数据（{days}天）...")
        
        order_id = 1
        order_detail_id = 1
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        total_orders = 0
        total_details = 0
        
        # 遍历每一天
        current_date = start_date
        while current_date <= end_date:
            orders_today = random.randint(*orders_per_day_range)
            
            for _ in range(orders_today):
                user_id = random.choice(self.user_ids)
                province_id = random.choice(self.province_ids)
                
                # 创建订单时间（当天随机时刻）
                hour = random.randint(0, 23)
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                create_time = current_date.replace(hour=hour, minute=minute, second=second)
                
                # 订单基本信息
                order_status = random.choice(['1001', '1002', '1003', '1004'])  # 订单状态
                payment_way = random.choice(['1101', '1102'])  # 支付方式
                
                # 订单详情：随机选择1-5个商品
                detail_count = random.randint(1, 5)
                order_total_amount = 0
                order_details = []
                
                for _ in range(detail_count):
                    # 热门商品有70%的概率，普通商品30%的概率
                    if random.random() < 0.7:
                        sku_id, sku_name, price = random.choice(self.hot_skus)
                    else:
                        sku_id, sku_name, price = random.choice(self.normal_skus)
                    
                    # 购买数量：热门商品购买更多
                    if sku_id in [s[0] for s in self.hot_skus[:3]]:  # 前3个热门商品
                        sku_num = random.randint(1, 8)  # 更多件数
                    elif sku_id in [s[0] for s in self.hot_skus]:
                        sku_num = random.randint(1, 5)
                    else:
                        sku_num = random.randint(1, 3)
                    
                    split_total_amount = round(price * sku_num, 2)
                    split_activity_amount = round(split_total_amount * random.uniform(0, 0.1), 2)
                    split_coupon_amount = round(split_total_amount * random.uniform(0, 0.05), 2)
                    final_amount = split_total_amount - split_activity_amount - split_coupon_amount
                    
                    order_total_amount += final_amount
                    
                    order_details.append({
                        'id': order_detail_id,
                        'order_id': order_id,
                        'sku_id': sku_id,
                        'sku_name': sku_name,
                        'order_price': price,
                        'sku_num': sku_num,
                        'split_total_amount': split_total_amount,
                        'split_activity_amount': split_activity_amount,
                        'split_coupon_amount': split_coupon_amount,
                        'create_time': create_time
                    })
                    order_detail_id += 1
                
                # 插入订单主表
                order_sql = """
                INSERT INTO order_info (
                    id, user_id, province_id, order_status, payment_way,
                    total_amount, original_total_amount, activity_reduce_amount,
                    coupon_reduce_amount, feight_fee, create_time, operate_time
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """
                total_reduce = sum(d['split_activity_amount'] + d['split_coupon_amount'] for d in order_details)
                original_amount = order_total_amount + total_reduce
                
                self.cursor.execute(order_sql, (
                    order_id, user_id, province_id, order_status, payment_way,
                    order_total_amount, original_amount,
                    sum(d['split_activity_amount'] for d in order_details),
                    sum(d['split_coupon_amount'] for d in order_details),
                    0.00, create_time, create_time
                ))
                
                # 插入订单明细表
                detail_sql = """
                INSERT INTO order_detail (
                    id, order_id, sku_id, sku_name, order_price, sku_num,
                    split_total_amount, split_activity_amount, split_coupon_amount,
                    create_time, operate_time
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """
                for detail in order_details:
                    self.cursor.execute(detail_sql, (
                        detail['id'], detail['order_id'], detail['sku_id'],
                        detail['sku_name'], detail['order_price'], detail['sku_num'],
                        detail['split_total_amount'], detail['split_activity_amount'],
                        detail['split_coupon_amount'], detail['create_time'], detail['create_time']
                    ))
                
                order_id += 1
                total_orders += 1
                total_details += len(order_details)
            
            # 每10天提交一次
            if (current_date - start_date).days % 10 == 0:
                self.conn.commit()
                print(f"  - 已生成到 {current_date.strftime('%Y-%m-%d')}")
            
            current_date += timedelta(days=1)
        
        self.conn.commit()
        print(f"✓ 历史订单数据生成完成")
        print(f"  - 订单数: {total_orders}")
        print(f"  - 订单明细数: {total_details}")
    
    def generate_recent_week_orders_by_province(self):
        """
        生成最近一周的省份特色订单（用于验证各省Top1商品）
        为每个省份注入特定的热销商品，形成地区特色
        """
        print("\n开始生成最近一周的省份特色订单...")
        
        # 获取当前最大订单ID
        self.cursor.execute("SELECT COALESCE(MAX(id), 0) FROM order_info")
        order_id = self.cursor.fetchone()[0] + 1
        
        self.cursor.execute("SELECT COALESCE(MAX(id), 0) FROM order_detail")
        order_detail_id = self.cursor.fetchone()[0] + 1
        
        # 定义各省份的特色热销商品（模拟地区消费偏好）
        province_favorites = {
            1: (1, '小米10 青春版 5G 6GB+64GB', 1999.00),  # 北京：小米手机
            2: (3, '小米10 青春版 5G 8GB+128GB', 2499.00),  # 天津：小米手机高配
            6: (13, 'ThinkPad X1 Carbon i5-10210U 8G 512G', 8999.00),  # 上海：商务笔记本
            7: (14, 'ThinkPad X1 Carbon i7-10510U 16G 512G', 11999.00),  # 江苏：商务笔记本高配
            8: (17, 'TCL 65V8 65英寸 4K超高清', 3999.00),  # 浙江：电视
            12: (18, 'TCL 75V8 75英寸 4K超高清', 5999.00),  # 山东：大屏电视
            19: (26, 'CAREMiLLE 口红 #01 正红色', 199.00),  # 甘肃：口红
            22: (27, 'CAREMiLLE 口红 #02 豆沙色', 199.00),  # 四川：口红
            25: (2, '小米10 青春版 5G 6GB+128GB', 2299.00),  # 广东：小米中配
        }
        
        # 为未定义特色的省份随机分配
        all_skus = self.hot_skus + self.normal_skus
        for pid in self.province_ids:
            if pid not in province_favorites:
                province_favorites[pid] = random.choice(all_skus)
        
        # 最近7天
        end_date = datetime.now()
        start_date = end_date - timedelta(days=6)
        
        total_orders = 0
        total_details = 0
        
        current_date = start_date
        while current_date <= end_date:
            # 每个省份每天生成20-50个该省特色商品订单
            for province_id in self.province_ids:
                orders_count = random.randint(20, 50)
                fav_sku_id, fav_sku_name, fav_price = province_favorites[province_id]
                
                for _ in range(orders_count):
                    user_id = random.choice(self.user_ids)
                    
                    # 创建时间
                    hour = random.randint(0, 23)
                    minute = random.randint(0, 59)
                    second = random.randint(0, 59)
                    create_time = current_date.replace(hour=hour, minute=minute, second=second)
                    
                    order_status = '1003'  # 已支付
                    payment_way = '1101'
                    
                    # 该省特色商品必定出现，且购买数量较多
                    sku_num = random.randint(2, 10)
                    split_total_amount = round(fav_price * sku_num, 2)
                    split_activity_amount = round(split_total_amount * 0.05, 2)
                    split_coupon_amount = round(split_total_amount * 0.03, 2)
                    final_amount = split_total_amount - split_activity_amount - split_coupon_amount
                    
                    # 插入订单
                    order_sql = """
                    INSERT INTO order_info (
                        id, user_id, province_id, order_status, payment_way,
                        total_amount, original_total_amount, activity_reduce_amount,
                        coupon_reduce_amount, feight_fee, create_time, operate_time
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    self.cursor.execute(order_sql, (
                        order_id, user_id, province_id, order_status, payment_way,
                        final_amount, split_total_amount, split_activity_amount,
                        split_coupon_amount, 0.00, create_time, create_time
                    ))
                    
                    # 插入订单明细
                    detail_sql = """
                    INSERT INTO order_detail (
                        id, order_id, sku_id, sku_name, order_price, sku_num,
                        split_total_amount, split_activity_amount, split_coupon_amount,
                        create_time, operate_time
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    self.cursor.execute(detail_sql, (
                        order_detail_id, order_id, fav_sku_id, fav_sku_name,
                        fav_price, sku_num, split_total_amount,
                        split_activity_amount, split_coupon_amount,
                        create_time, create_time
                    ))
                    
                    order_id += 1
                    order_detail_id += 1
                    total_orders += 1
                    total_details += 1
            
            self.conn.commit()
            print(f"  - 已生成 {current_date.strftime('%Y-%m-%d')} 的省份特色订单")
            current_date += timedelta(days=1)
        
        print(f"✓ 最近一周省份特色订单生成完成")
        print(f"  - 订单数: {total_orders}")
        print(f"  - 订单明细数: {total_details}")
    
    def generate_province_top3_diversity(self):
        """
        生成各省Top3商品的多样性数据
        确保每个省份都有3个以上的热门商品，形成明显的Top3排名
        """
        print("\n开始生成各省Top3商品多样性数据...")
        
        # 获取当前最大订单ID
        self.cursor.execute("SELECT COALESCE(MAX(id), 0) FROM order_info")
        order_id = self.cursor.fetchone()[0] + 1
        
        self.cursor.execute("SELECT COALESCE(MAX(id), 0) FROM order_detail")
        order_detail_id = self.cursor.fetchone()[0] + 1
        
        # 为每个省份定义Top3商品及其购买权重
        province_top3 = {}
        for province_id in self.province_ids:
            # 从所有SKU中随机选择3个作为该省Top3，并分配不同的购买权重
            selected = random.sample(self.hot_skus + self.normal_skus, 3)
            province_top3[province_id] = [
                (selected[0], random.randint(80, 120)),   # Top1: 高购买量
                (selected[1], random.randint(50, 79)),    # Top2: 中购买量
                (selected[2], random.randint(30, 49)),    # Top3: 较低购买量
            ]
        
        # 生成过去30天的数据，让Top3更明显
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        total_orders = 0
        total_details = 0
        
        for province_id in self.province_ids:
            for (sku_id, sku_name, price), order_count in province_top3[province_id]:
                # 为该省该商品生成指定数量的订单
                for _ in range(order_count):
                    user_id = random.choice(self.user_ids)
                    
                    # 随机时间
                    days_ago = random.randint(0, 30)
                    create_time = start_date + timedelta(
                        days=days_ago,
                        hours=random.randint(0, 23),
                        minutes=random.randint(0, 59),
                        seconds=random.randint(0, 59)
                    )
                    
                    order_status = '1003'
                    payment_way = random.choice(['1101', '1102'])
                    
                    # 购买数量
                    sku_num = random.randint(1, 6)
                    split_total_amount = round(price * sku_num, 2)
                    split_activity_amount = round(split_total_amount * random.uniform(0, 0.08), 2)
                    split_coupon_amount = round(split_total_amount * random.uniform(0, 0.04), 2)
                    final_amount = split_total_amount - split_activity_amount - split_coupon_amount
                    
                    # 插入订单
                    order_sql = """
                    INSERT INTO order_info (
                        id, user_id, province_id, order_status, payment_way,
                        total_amount, original_total_amount, activity_reduce_amount,
                        coupon_reduce_amount, feight_fee, create_time, operate_time
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    self.cursor.execute(order_sql, (
                        order_id, user_id, province_id, order_status, payment_way,
                        final_amount, split_total_amount, split_activity_amount,
                        split_coupon_amount, 0.00, create_time, create_time
                    ))
                    
                    # 插入订单明细
                    detail_sql = """
                    INSERT INTO order_detail (
                        id, order_id, sku_id, sku_name, order_price, sku_num,
                        split_total_amount, split_activity_amount, split_coupon_amount,
                        create_time, operate_time
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    self.cursor.execute(detail_sql, (
                        order_detail_id, order_id, sku_id, sku_name,
                        price, sku_num, split_total_amount,
                        split_activity_amount, split_coupon_amount,
                        create_time, create_time
                    ))
                    
                    order_id += 1
                    order_detail_id += 1
                    total_orders += 1
                    total_details += 1
            
            # 每处理5个省份提交一次
            if province_id % 5 == 0:
                self.conn.commit()
                print(f"  - 已生成前 {province_id} 个省份的Top3数据")
        
        self.conn.commit()
        print(f"✓ 各省Top3商品多样性数据生成完成")
        print(f"  - 订单数: {total_orders}")
        print(f"  - 订单明细数: {total_details}")
    
    def show_statistics(self):
        """显示生成的数据统计信息"""
        print("\n" + "="*60)
        print("数据统计信息")
        print("="*60)
        
        # 订单总数
        self.cursor.execute("SELECT COUNT(*) FROM order_info")
        total_orders = self.cursor.fetchone()[0]
        print(f"订单总数: {total_orders}")
        
        # 订单明细总数
        self.cursor.execute("SELECT COUNT(*) FROM order_detail")
        total_details = self.cursor.fetchone()[0]
        print(f"订单明细总数: {total_details}")
        
        # 商品总件数
        self.cursor.execute("SELECT SUM(sku_num) FROM order_detail")
        total_sku_num = self.cursor.fetchone()[0]
        print(f"商品总件数: {total_sku_num}")
        
        # 涉及商品数
        self.cursor.execute("SELECT COUNT(DISTINCT sku_id) FROM order_detail")
        distinct_skus = self.cursor.fetchone()[0]
        print(f"涉及商品数: {distinct_skus}")
        
        # 涉及省份数
        self.cursor.execute("SELECT COUNT(DISTINCT province_id) FROM order_info")
        distinct_provinces = self.cursor.fetchone()[0]
        print(f"涉及省份数: {distinct_provinces}")
        
        # 时间范围
        self.cursor.execute("SELECT MIN(create_time), MAX(create_time) FROM order_info")
        min_time, max_time = self.cursor.fetchone()
        print(f"时间范围: {min_time} ~ {max_time}")
        
        # Top5商品（按总件数）
        print("\nTop5 商品（按历史总件数）:")
        self.cursor.execute("""
            SELECT sku_id, sku_name, SUM(sku_num) as total_num
            FROM order_detail
            GROUP BY sku_id, sku_name
            ORDER BY total_num DESC
            LIMIT 5
        """)
        for i, (sku_id, sku_name, total_num) in enumerate(self.cursor.fetchall(), 1):
            print(f"  {i}. SKU#{sku_id} {sku_name}: {total_num}件")
        
        # 最近7天各省Top1示例（前5个省份）
        print("\n最近7天各省Top1商品示例（前5个省份）:")
        self.cursor.execute("""
            SELECT 
                province_id,
                sku_id,
                sku_name,
                SUM(sku_num) as total_num
            FROM order_info oi
            JOIN order_detail od ON oi.id = od.order_id
            WHERE oi.create_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)
            GROUP BY province_id, sku_id, sku_name
            ORDER BY province_id, total_num DESC
        """)
        
        current_province = None
        shown = 0
        for province_id, sku_id, sku_name, total_num in self.cursor.fetchall():
            if province_id != current_province:
                if shown >= 5:
                    break
                current_province = province_id
                shown += 1
                print(f"  省份#{province_id}: SKU#{sku_id} {sku_name} - {total_num}件")
        
        print("="*60)


def main():
    """主函数"""
    print("="*60)
    print("GMALL订单数据生成工具")
    print("="*60)
    
    generator = OrderDataGenerator()
    
    try:
        # 连接数据库
        generator.connect()
        
        # 询问是否清空现有数据
        print("\n⚠️  警告：此操作将清空现有的订单数据！")
        confirm = input("是否继续？(yes/no): ").strip().lower()
        
        if confirm != 'yes':
            print("操作已取消")
            return
        
        # 清空现有数据
        generator.clear_existing_orders()
        
        # 生成数据
        print("\n" + "="*60)
        print("开始生成测试数据")
        print("="*60)
        
        # 1. 生成90天的历史订单（用于历史总件数最多指标）
        generator.generate_historical_orders(days=90, orders_per_day_range=(80, 150))
        
        # 2. 生成最近7天的省份特色订单（用于各省7天内Top1指标）
        generator.generate_recent_week_orders_by_province()
        
        # 3. 生成各省Top3商品的多样性数据（用于各省历史Top3指标）
        generator.generate_province_top3_diversity()
        
        # 显示统计信息
        generator.show_statistics()
        
        print("\n✅ 数据生成完成！")
        print("\n接下来可以运行以下命令测试ADS指标：")
        print("  python hive.py dws_to_ads")
        print("或运行完整流水线：")
        print("  python main.py")
        
    except Exception as e:
        print(f"\n❌ 数据生成失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        generator.close()


if __name__ == "__main__":
    main()
