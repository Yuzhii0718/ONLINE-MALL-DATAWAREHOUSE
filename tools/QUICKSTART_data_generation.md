# 快速测试指令

## 生成订单数据并验证 ADS 指标

```powershell
# 1. 激活环境
conda activate pyspark310

# 2. 生成测试数据
python tools/generate_order_data.py
# 输入 'yes' 确认

# 3. 运行完整流水线（推荐）
python main.py

# 或者单独运行 DWS->ADS 步骤
python hive.py dws_to_ads
```

## 验证结果

数据生成后，可在 MySQL `gmall_hive` 数据库中查看结果表：

```sql
-- 1. 历史被下单总件数最多的商品（应该只有1条记录）
SELECT * FROM gmall_hive.ads_top_sku_by_total_num;

-- 2. 最近一周各省份下单件数最多的商品（应该有34条记录，每省1条）
SELECT * FROM gmall_hive.ads_top_sku_by_province_7d
ORDER BY order_num_7d DESC
LIMIT 10;

-- 3. 每个省份下单商品数量前三（应该有34*3=102条记录）
SELECT * FROM gmall_hive.ads_top3_sku_by_province
WHERE province_id IN (1, 6, 8, 12)  -- 查看北京、上海、浙江、山东
ORDER BY province_id, rank;
```

## 预期结果示例

### ads_top_sku_by_total_num

| sku_id | sku_name | order_num | order_count | order_amount |
|--------|----------|-----------|-------------|--------------|
| 1 | 小米10 青春版 5G 6GB+64GB | 12458 | 5230 | 24,904,542.00 |

### ads_top_sku_by_province_7d（前3条）

| province_id | province_name | sku_id | sku_name | order_num_7d |
|-------------|---------------|--------|----------|--------------|
| 1 | 北京 | 1 | 小米10 青春版 5G 6GB+64GB | 1523 |
| 2 | 天津 | 3 | 小米10 青春版 5G 8GB+128GB | 1387 |
| 6 | 上海 | 13 | ThinkPad X1 Carbon i5 8G 512G | 1256 |

### ads_top3_sku_by_province（北京示例）

| province_id | province_name | sku_id | sku_name | order_num_total | rank |
|-------------|---------------|--------|----------|-----------------|------|
| 1 | 北京 | 1 | 小米10 青春版 5G 6GB+64GB | 2845 | 1 |
| 1 | 北京 | 14 | ThinkPad X1 Carbon i7 16G 512G | 1652 | 2 |
| 1 | 北京 | 17 | TCL 65V8 65英寸 4K | 1234 | 3 |
