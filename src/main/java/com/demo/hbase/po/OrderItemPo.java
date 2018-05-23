package com.demo.hbase.po;

import java.math.BigDecimal;

import lombok.Data;

@Data
public class OrderItemPo {

	private String gooId;

	private Integer count;

	private String gooName;

	private BigDecimal gooPrice;

}
