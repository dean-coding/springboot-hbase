package com.demo.hase;

import java.math.BigDecimal;

import lombok.Data;

@Data
public class OrderItemDaoSample {

	private Long id;

	private Long gooId;
	
	private Integer count;

	private String gooName;

	private String gooTitle;

	private String gooPic;

	private BigDecimal gooPrice;

}
