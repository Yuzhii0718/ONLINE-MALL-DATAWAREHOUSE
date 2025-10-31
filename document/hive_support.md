# Hive 支持

当配置文件中启用 enable_hive_support 选项时，系统将使用 Hive 代替 Spark 进行数据存储和查询操作。

```plaintext
MySQL业务数据
    ↓
ODS层（原始数据层）
    ↓
DIM层（维度数据层） + DWD层（明细数据层）
    ↓
DWS层（轻度聚合层）
    ↓
ADS层（应用数据服务层）
    ↓
MySQL结果数据 → Superset可视化
```

主要运行 hive.py(与main.py类似)，启用 Hive 支持后，运行 `python main.py` 将自动调用 hive.py 进行数据处理和存储。

相关文件

```plaintext
GMALL-Data-Warehouse/
├── hive/                      # Hive相关脚本
│   ├── ads_category              # Hive ADS层分类指标脚本文件夹
|   └──xxx.py               # 其他Hive脚本文件
├── main.py                     # 主运行脚本
├── hive.py            # Hive支持脚本
└── ...
```
