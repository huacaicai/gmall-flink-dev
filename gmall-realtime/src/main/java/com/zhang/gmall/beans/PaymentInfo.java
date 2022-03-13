package com.zhang.gmall.beans;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @title:
 * @author: zhang
 * @date: 2022/3/10 09:50
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
