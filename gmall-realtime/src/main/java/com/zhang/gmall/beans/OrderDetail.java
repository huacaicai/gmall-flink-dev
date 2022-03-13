package com.zhang.gmall.beans;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @title: 订单明细实体类
 * @author: zhang
 * @date: 2022/3/7 18:11
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
