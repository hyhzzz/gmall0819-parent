package com.atguigu.gmall.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @author coderhyh
 * @create 2022-04-22 12:50
 * 订单明细实体类
 */
@Data
public class OrderDetail {
   Long id;
   Long order_id ;
   Long sku_id;
   BigDecimal order_price ;
   Long sku_num ;
   String sku_name;
   String create_time;
   BigDecimal split_total_amount;
   BigDecimal split_activity_amount;
   BigDecimal split_coupon_amount;
   Long create_ts;
}