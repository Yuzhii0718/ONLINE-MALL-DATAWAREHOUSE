---
title: 表结构定义
---

## 索引

- ODS 层表结构

4.1 活动信息表
4.2 活动规则表
4.3 一级品类表
4.4 二级品类表
4.5 三级品类表
4.6 编码字典表
4.7 省份表
4.8 地区表
4.9 品牌表
4.10 购物车表
4.11 优惠券信息表
4.12 商品平台属性表
4.13 商品表
4.14 商品销售属性值表
4.15 SPU表
4.16 营销坑位表
4.17 营销渠道表
4.18 购物车表
4.19 评论表
4.20 优惠券领用表
4.21 收藏表
4.22 订单明细表
4.23 订单明细活动关联表
4.24 订单明细优惠券关联表
4.25 订单表
4.26 退单表
4.28 支付表
4.29 退款表
4.30 用户表

- DIM 层表结构

5.1 商品维度表
5.2 优惠券维度表
5.3 活动维度表
8.3 活动维度表
8.4 地区维度表
8.5 营销坑位维度表
8.6 营销渠道维度表
8.7 日期维度表
8.8 用户维度表

- DWD 层表结构

6.1 交易域加购事务事实表
6.2 交易域下单事务事实表
6.3 交易域支付成功事务事实表
6.4 交易域购物车周期快照事实表
6.5 交易域交易流程累积快照事实表
6.6 工具域优惠券使用(支付)事务事实表
6.7 互动域收藏商品事务事实表
6.8 流量域页面浏览事务事实表
6.9 用户域用户注册事务事实表
6.10 用户域用户登录事务事实表

- DWS 层表结构

7.1 最近1日汇总表
7.2 最近n日汇总表
7.3 历史至今汇总表

- ADS 层表结构

8.1 流量主题
8.2 用户主题
8.3 商品主题
8.4 交易主题
8.5 优惠券主题

### 3.3.1 活动信息表（activity_info）

| 字段名 | 字段说明 |
|--------|----------|
| id | 活动id |
| activity_name | 活动名称 |
| activity_type | 活动类型（1：满减，2：折扣） |
| activity_desc | 活动描述 |
| start_time | 开始时间 |
| end_time | 结束时间 |
| create_time | 创建时间 |
| operate_time | 修改时间 |

### 3.3.2 活动规则表（activity_rule）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| activity_id | 活动ID |
| activity_type | 活动类型 |
| condition_amount | 满减金额 |
| condition_num | 满减件数 |
| benefit_amount | 优惠金额 |
| benefit_discount | 优惠折扣 |
| benefit_level | 优惠级别 |
| create_time | 创建时间 |
| operate_time | 修改时间 |

### 3.3.3 活动商品关联表（activity_sku）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| activity_id | 活动id |
| sku_id | sku_id |
| create_time | 创建时间 |

### 3.3.4 平台属性表（base_attr_info）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| attr_name | 属性名称 |
| category_id | 分类id |
| category_level | 分类层级 |

### 3.3.5 平台属性值表（base_attr_value）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| value_name | 属性值名称 |
| attr_id | 属性id |

### 3.3.6 一级分类表（base_category1）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| name | 分类名称 |
| create_time | 创建时间 |
| operate_time | 修改时间 |

### 3.3.7 二级分类表（base_category2）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| name | 二级分类名称 |
| category1_id | 一级分类编号 |
| create_time | 创建时间 |
| operate_time | 修改时间 |

### 3.3.8 三级分类表（base_category3）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| name | 三级分类名称 |
| category2_id | 二级分类编号 |
| create_time | 创建时间 |
| operate_time | 修改时间 |

### 3.3.9 字典表（base_dic）

| 字段名 | 字段说明 |
|--------|----------|
| dic_code | 编号 |
| dic_name | 编码名称 |
| parent_code | 父编号 |
| create_time | 创建日期 |
| operate_time | 修改日期 |

### 3.3.10 省份表（base_province）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| name | 省名称 |
| region_id | 大区id |
| area_code | 行政区位码 |
| iso_code | 国际编码 |
| iso_3166_2 | ISO3166编码 |
| create_time | 创建时间 |
| operate_time | 修改时间 |

### 3.3.11 地区表（base_region）

| 字段名 | 字段说明 |
|--------|----------|
| id | 大区id |
| region_name | 大区名称 |
| create_time | 创建时间 |
| operate_time | 修改时间 |

### 3.3.12 品牌表（base_trademark）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| tm_name | 品牌名称 |
| logo_url | 品牌logo的图片路径 |
| create_time | 创建时间 |
| operate_time | 修改时间 |

### 3.3.13 购物车表（cart_info）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| user_id | 用户id |
| sku_id | skuid |
| cart_price | 放入购物车时价格 |
| sku_num | 数量 |
| img_url | 图片文件 |
| sku_name | sku名称 (冗余) |
| is_checked | 是否已经下单 |
| create_time | 创建时间 |
| operate_time | 修改时间 |
| is_ordered | 是否已经下单 |
| order_time | 下单时间 |
| source_type | 来源类型 |
| source_id | 来源编号 |

### Banner 表（cms_banner）（冗余）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| title | 标题 |
| image_url | 图片地址 |
| link_url | 链接地址 |
| sort | 排序 |
| create_time | 创建时间 |
| operate_time | 修改时间 |

### 3.3.14 评价表（comment_info）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| user_id | 用户id |
| nick_name | 用户昵称 |
| head_img | 图片 |
| sku_id | 商品sku_id |
| spu_id | 商品spu_id |
| order_id | 订单编号 |
| appraise | 评价 1 好评 2 中评 3 差评 |
| comment_txt | 评价内容 |
| create_time | 创建时间 |
| operate_time | 修改时间 |

### 3.3.15 优惠券信息表（coupon_info）

| 字段名 | 字段说明 |
|--------|----------|
| id | 购物券编号 |
| coupon_name | 购物券名称 |
| coupon_type | 购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券 |
| condition_amount | 满额数（3） |
| condition_num | 满件数（4） |
| activity_id | 活动编号 |
| benefit_amount | 减金额（1 3） |
| benefit_discount | 折扣（2 4） |
| create_time | 创建时间 |
| range_type | 范围类型 1、商品(spuid) 2、品类(三级分类id) 3、品牌 |
| limit_num | 最多领用次数 |
| taken_count | 已领用次数 |
| start_time | 可以领取的开始日期 |
| end_time | 可以领取的结束日期 |
| operate_time | 修改时间 |
| expire_time | 过期时间 |
| range_desc | 范围描述 |

### 3.3.16 优惠券优惠范围表（coupon_range）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| coupon_id | 优惠券id |
| range_type | 范围类型 1、商品(spuid) 2、品类(三级分类id) 3、品牌 |
| range_id | 范围id |

### 3.3.17 优惠券领用表（coupon_use）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| coupon_id | 购物券id |
| user_id | 用户id |
| order_id | 订单id |
| coupon_status | 购物券状态（1：未使用 2：已使用） |
| get_time | 获取时间 |
| using_time | 使用时间 |
| used_time | 支付时间 |
| expire_time | 过期时间 |

### 3.3.18 收藏表（favor_info）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| user_id | 用户id |
| sku_id | skuid |
| spu_id | 商品id |
| is_cancel | 是否已取消 0 正常 1 已取消 |
| create_time | 创建时间 |
| cancel_time | 修改时间 |

### 金融 SKU 花费表（finance_sku_cost）（冗余）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| sku_id | sku_id |
| sku_name | sku名称（冗余） |
| busi_date | 业务日期 |
| is_lastest | 是否最新（1：是 0：否） |
| sku_cost | sku成本 |
| create_time | 创建时间 |

### 3.3.19 订单明细表（order_detail）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| order_id | 订单编号 |
| sku_id | sku_id |
| sku_name | sku名称（冗余） |
| img_url | 图片名称（冗余） |
| order_price | 购买价格（下单时sku价格） |
| sku_num | 购买个数 |
| create_time | 创建时间 |
| source_type | 来源类型 |
| source_id | 来源编号 |
| split_total_amount | 分摊总金额 |
| split_activity_amount | 分摊活动减免金额 |
| split_coupon_amount | 分摊优惠券减免金额 |

### 3.3.20 订单明细活动关联表（order_detail_activity）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| order_id | 订单id |
| order_detail_id | 订单明细id |
| activity_id | 活动id |
| activity_rule_id | 活动规则 |
| sku_id | skuid |
| create_time | 获取时间 |

### 3.3.21 订单明细优惠券关联表（order_detail_coupon）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| order_id | 订单id |
| order_detail_id | 订单明细id |
| coupon_id | 购物券id |
| coupon_use_id | 购物券领用id |
| sku_id | skuid |
| create_time | 获取时间 |

### 3.3.22 订单表（order_info）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| consignee | 收货人 |
| consignee_tel | 收件人电话 |
| total_amount | 总金额 |
| order_status | 订单状态 |
| user_id | 用户id |
| payment_way | 付款方式 |
| delivery_address | 送货地址 |
| order_comment | 订单备注 |
| out_trade_no | 订单交易编号（第三方支付用） |
| trade_body | 订单描述（第三方支付用） |
| create_time | 创建时间 |
| operate_time | 操作时间 |
| expire_time | 失效时间 |
| process_status | 进度状态 |
| tracking_no | 物流单编号 |
| parent_order_id | 父订单编号 |
| img_url | 图片路径 |
| province_id | 地区 |
| activity_reduce_amount | 促销金额 |
| coupon_reduce_amount | 优惠金额 |
| original_total_amount | 原价金额 |
| feight_fee | 运费 |
| feight_fee_reduce | 运费减免 |
| refundable_time | 可退款日期（签收后30天） |

### 3.3.23 退单表（order_refund_info）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| user_id | 用户id |
| order_id | 订单id |
| sku_id | skuid |
| refund_type | 退款类型 |
| refund_num | 退货件数 |
| refund_amount | 退款金额 |
| refund_reason_type | 原因类型 |
| refund_reason_txt | 原因内容 |
| refund_status | 退款状态（0：待审批 1：已退款） |
| create_time | 创建时间 |

### 3.3.24 订单状态流水表（order_status_log）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| order_id | 订单编号 |
| order_status | 订单状态 |
| operate_time | 操作时间 |

### 3.3.25 支付表（payment_info）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| out_trade_no | 对外业务编号 |
| order_id | 订单编号 |
| user_id | 用户id |
| payment_type | 支付类型（微信 支付宝） |
| trade_no | 交易编号 |
| total_amount | 支付金额 |
| subject | 交易内容 |
| payment_status | 支付状态 |
| create_time | 创建时间 |
| callback_time | 回调时间 |
| callback_content | 回调信息 |

### 3.3.26 退款表（refund_payment）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| out_trade_no | 对外业务编号 |
| order_id | 订单编号 |
| sku_id | 商品sku_id |
| payment_type | 支付类型（微信 支付宝） |
| trade_no | 交易编号 |
| total_amount | 退款金额 |
| subject | 交易内容 |
| refund_status | 退款状态 |
| create_time | 创建时间 |
| callback_time | 回调时间 |
| callback_content | 回调信息 |

### 秒杀商品表（seckill_goods）（冗余）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| sku_id | 商品sku_id |
| spu_id | 商品spu_id |
| sku_name | 商品名称 |
| sku_default_img | 商品默认图片 |
| price | 秒杀价格 |
| cost_price | 成本价格 |
| create_time | 创建时间 |
| check_time | 审核时间 |
| status | 状态（1：未审核 2：审核通过 3：审核不通过） |
| start_time | 秒杀开始时间 |
| end_time | 秒杀结束时间 |
| num | 秒杀数量 |
| stock_count | 库存数量 |
| sku_desc | 商品规格描述 |

### 3.3.27 SKU平台属性表（sku_attr_value）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| attr_id | 属性id（冗余） |
| value_id | 属性值id |
| sku_id | skuid |
| attr_name | 属性名称 |
| value_name | 属性值名称 |
| create_time | 创建时间 |
| operate_time | 修改时间 |

### sku 图片表（sku_image）（冗余）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| sku_id | skuid |
| img_name | 图片名称 |
| img_url | 图片路径 |
| spu_img_id | 商品图片id（冗余） |
| is_default | 是否默认图片（1：是 0：否） |
| create_time | 创建时间 |
| operate_time | 修改时间 |

### 3.3.28 SKU信息表（sku_info）

| 字段名 | 字段说明 |
|--------|----------|
| id | 库存id（itemID） |
| spu_id | 商品id |
| price | 价格 |
| sku_name | sku名称 |
| sku_desc | 商品规格描述 |
| weight | 重量 |
| tm_id | 品牌（冗余） |
| category3_id | 三级分类id（冗余） |
| sku_default_img | 默认显示图片（冗余） |
| is_sale | 是否销售（1：是 0：否） |
| create_time | 创建时间 |
| operate_time | 修改时间 |

### 3.3.29 SKU销售属性表（sku_sale_attr_value）

| 字段名 | 字段说明 |
|--------|----------|
| id | id |
| sku_id | 库存单元id |
| spu_id | spu_id（冗余） |
| sale_attr_value_id | 销售属性值id |
| sale_attr_id | 销售属性id |
| sale_attr_name | 销售属性值名称 |
| sale_attr_value_name | 销售属性值名称 |
| create_time | 创建时间 |
| operate_time | 修改时间 |

### 3.3.30 SPU信息表（spu_info）

| 字段名 | 字段说明 |
|--------|----------|
| id | 商品id |
| spu_name | 商品名称 |
| description | 商品描述（后台简述） |
| category3_id | 三级分类id |
| tm_id | 品牌id |
| create_time | 创建时间 |
| operate_time | 修改时间 |

### SPU poster（spu_poster）（冗余）

| 字段名 | 字段说明 |
|--------|----------|
| id | id |
| spu_id | spu_id（冗余） |
| img_url | 图片地址 |
| img_name | 图片名称 |
| is_deleted | 是否默认（1：是 0：否） |
| create_time | 创建时间 |
| operate_time | 修改时间 |

### 3.3.31 SPU销售属性表（spu_sale_attr）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号（业务中无关联） |
| spu_id | 商品id |
| base_sale_attr_id | 销售属性id |
| sale_attr_name | 销售属性名称（冗余） |

### 3.3.32 SPU销售属性值表（spu_sale_attr_value）

| 字段名 | 字段说明 |
|--------|----------|
| id | 销售属性值编号 |
| spu_id | 商品id |
| base_sale_attr_id | 销售属性id |
| sale_attr_value_name | 销售属性值名称 |
| sale_attr_name | 销售属性名称（冗余） |

### 3.3.33 用户地址表（user_address）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| user_id | 用户id |
| province_id | 省份id |
| user_address | 用户地址 |
| consignee | 收件人 |
| phone_num | 联系方式 |
| is_default | 是否是默认 |

### 3.3.34 用户信息表（user_info）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| login_name | 用户名称 |
| nick_name | 用户昵称 |
| passwd | 用户密码 |
| name | 用户姓名 |
| phone_num | 手机号 |
| email | 邮箱 |
| head_img | 头像 |
| user_level | 用户级别 |
| birthday | 用户生日 |
| gender | 性别 M男,F女 |
| create_time | 创建时间 |
| operate_time | 修改时间 |
| status | 状态 |

### ware info（冗余）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| name | 名称 |
| address | 地址 |
| areacode | 区域编码 |

### ware_order_task（冗余）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| order_id | 订单id |
| consignee | 收货人 |
| consignee_tel | 收货人电话 |
| delivery_address | 送货地址 |
| order_comment | 订单备注 |
| payment_way | 付款方式 |
| order_body | 订单描述 |
| order_body | 订单内容 |
| tracking_no | 物流单号 |
| create_time | 创建时间 |
| ware_id | 仓库id |
| task_comment | 任务备注 |

### ware_order_task_detail（冗余）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| sku_id | sku_id |
| sku_name | sku名称 |
| sku_num | sku数量 |
| task_id | 任务id |
| refund_status | 退款状态 |

### ware_sku（冗余）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| sku_id | sku_id |
| warehouse_id | 仓库id |
| stock | 库存 |
| stock_name | 库存名称 |
| stock_lock | 库存锁定 |

### z_log （冗余）

| 字段名 | 字段说明 |
|--------|----------|
| id | 编号 |
| log | 日志内容 |
