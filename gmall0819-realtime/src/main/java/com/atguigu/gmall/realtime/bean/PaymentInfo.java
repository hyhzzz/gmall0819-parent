package com.atguigu.gmall.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @author coderhyh
 * @create 2022-04-27 1:11
 *  支付信息实体类
 */
@Data
public class PaymentInfo {
   Long id;
   Long order_id;
   Long user_id;
   BigDecimal total_amount;
   String subject;
   String payment_type;
   String create_time;
   String callback_time;
}