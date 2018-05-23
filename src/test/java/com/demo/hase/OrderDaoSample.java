package com.demo.hase;

import java.util.Date;
import java.util.List;

import lombok.Data;

@Data
public class OrderDaoSample {

	private String trackingNumber; // 快递单号

	private String paymentMode; // 支付方式

	private String paymentNumber; // 支付编号（存储微信支付编号、支付宝支付编号）

	private Date paymentTimeExpire; // 支付结束时间

	private String fromUser; // 持有方

	private String fromAddress; // 发货地址

	private String fromAddressDetails; // 发货详细地址

	private String fromAddressPostalCode; // 发货地址邮编

	private String toUser; // 需求方

	private String toAddress; // 收货地址

	private String toAddressDetails; // 收货详细地址

	private String toAddressPostalCode; // 收货地址邮件

	private Integer status; // 状态

	private List<OrderItemDaoSample> items;

	private Date notifyDeliveryDate; // 提醒发货时间（为空表示尚未提醒）

	private Integer extendAutoCompleteTimes = 0; // 延长自动完成次数

	private Date autoCompleteDate; // 自动完成日期

	private Date paymentDate; // 支付时间

	private Date expectDeliveryDate; // 期望发货日期

	private Date deliveryDate; // 发货时间

	private Date completedDate; // 完成时间

	private Date createdDate;

	private Date lastModifiedDate;
}
