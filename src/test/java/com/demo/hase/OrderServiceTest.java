package com.demo.hase;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.TableName;
import org.junit.Test;

import com.demo.hbase.HbaseOperateUtils;
import com.demo.hbase.entity.PageResult;
import com.demo.hbase.order.OrderService;
import com.demo.hbase.order.OrderServiceHbaseImpl;
import com.demo.hbase.po.OrderHTable;
import com.demo.hbase.po.OrderItemHTable;
import com.demo.hbase.po.OrderItemPo;
import com.demo.hbase.po.OrderPo;

public class OrderServiceTest {

	private OrderService orderService = new OrderServiceHbaseImpl();

	@Test
	public void testPrintAll() {
		HbaseOperateUtils.printAll(TableName.valueOf(OrderHTable.T_NAME));
		System.out.println();
		 HbaseOperateUtils.printAll(TableName.valueOf(OrderItemHTable.T_NAME));

	}

	@Test
	public void testDeleteAll() throws Exception {
		HbaseOperateUtils.deleteTable(TableName.valueOf(OrderHTable.T_NAME));
		HbaseOperateUtils.deleteTable(TableName.valueOf(OrderItemHTable.T_NAME));
	}

	@Test
	public void testCreateOrder() throws Exception {
		for (int i = 0; i < 5; i++) {
			createMockOrder();
		}
	}

	private void createMockOrder() {
		OrderPo orderPo = new OrderPo();
		orderPo.setFromUser(RandomStringUtils.randomAlphabetic(3));
		orderPo.setToUser(RandomStringUtils.randomAlphabetic(3));
		
		List<OrderItemPo> items = new ArrayList<>();
		for (int j = 0; j < 2; j++) {
			OrderItemPo item = new OrderItemPo();
			item.setCount(j);
			item.setGooPrice(new BigDecimal(100 + j));
			item.setGooName("good-" + j);
			items.add(item);
		}
		orderPo.setItems(items);
		orderService.createOrder(orderPo);
	}

	@Test
	public void testQueryPage() throws Exception {
		PageResult<OrderPo> queryPage = orderService.queryPage(0, 10);
		System.out.printf("分页查询page=[%d],size=[%s]的总数为=[%d]结果为:%n", queryPage.getPage(), queryPage.getSize(),
				queryPage.getTotal());
		queryPage.getDatas().stream().forEach(System.out::println);
	}

//	@Test
//	public void testName() throws Exception {
//		orderService.deleteOrder("XcL1527064088676lS1o");
//	}
}
