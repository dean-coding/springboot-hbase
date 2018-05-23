package com.demo.hbase.po;

import java.util.List;
import java.util.Set;

import lombok.Data;

@Data
public class OrderPo {

	private String orderCode; // 订单号

	private String fromUser; 

	private String toUser;

	private List<OrderItemPo> items;

	private Set<String> itemIds;
	
	private String status; 
	
	private String remark;

}
