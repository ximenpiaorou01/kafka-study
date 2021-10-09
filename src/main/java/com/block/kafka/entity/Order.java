package com.block.kafka.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author wangrongsong
 * @title: Order
 * @projectName kafka-study
 * @description: TODO
 * @date 2021-10-08 15:50
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order {
    private Long orderId;
    private int count;
}
