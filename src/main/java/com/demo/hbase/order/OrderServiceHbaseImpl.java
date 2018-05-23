package com.demo.hbase.order;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.demo.hbase.HbaseOperateUtils;
import com.demo.hbase.RedisTools;
import com.demo.hbase.entity.PageResult;
import com.demo.hbase.po.OrderHTable;
import com.demo.hbase.po.OrderItemHTable;
import com.demo.hbase.po.OrderItemPo;
import com.demo.hbase.po.OrderPo;

import redis.clients.jedis.Jedis;

public class OrderServiceHbaseImpl implements OrderService {

	private Jedis jedis = RedisTools.getJedis();

	private static final TableName orderTableN = TableName.valueOf(OrderHTable.T_NAME);
	private static final TableName orderItemTableN = TableName.valueOf(OrderItemHTable.T_NAME);
	private static final String R_KEY_FOR_ORDER = "_R_KEY_FOR_ORDER";
	private static final String GOO_ID_PREFIX = "GOO";

	static {
		try {
			HbaseOperateUtils.createTableIfNotExits(Arrays.asList(OrderHTable.F_INFO, OrderHTable.F_DETAIL),
					orderTableN);
			HbaseOperateUtils.createTableIfNotExits(Arrays.asList(OrderItemHTable.F_INFO), orderItemTableN);
		} catch (IOException e) {
			System.err.println(OrderServiceHbaseImpl.class.getName() + "->表创建失败");
			e.printStackTrace();
		}
	}

	@Override
	public String createOrder(OrderPo orderDao) {

		final String fromUser = orderDao.getFromUser();
		String orderCode = genOrderCode(fromUser);
		Put orderRow = new Put(Bytes.toBytes(orderCode));
		HbaseOperateUtils.addStringCol(orderRow, OrderHTable.F_INFO, OrderHTable.INFO_C_ORDER_CODE, orderCode);
		HbaseOperateUtils.addStringCol(orderRow, OrderHTable.F_INFO, OrderHTable.INFO_C_FROM_USER, fromUser);
		HbaseOperateUtils.addStringCol(orderRow, OrderHTable.F_INFO, OrderHTable.INFO_C_TO_USER, orderDao.getToUser());
		HbaseOperateUtils.addStringCol(orderRow, OrderHTable.F_INFO, OrderHTable.INFO_C_STATUS, "0");
		HbaseOperateUtils.addStringCol(orderRow, OrderHTable.F_DETAIL, OrderHTable.DETAIL_C_REMARK, "remark...");

		List<OrderItemPo> items = orderDao.getItems();
		Assert.notNull(items, "the item of order must be not null");
		List<Put> itemRows = new ArrayList<>(items.size());
		List<String> itemIds = new ArrayList<>(items.size());
		items.stream().forEach(item -> {
			String gooId = genOrderCode(fromUser + GOO_ID_PREFIX);
			itemIds.add(gooId);
			Put itemRow = new Put(Bytes.toBytes(gooId));
			HbaseOperateUtils.addStringCol(itemRow, OrderItemHTable.F_INFO, OrderItemHTable.INFO_C_GOO_ID, gooId);
			HbaseOperateUtils.addStringCol(itemRow, OrderItemHTable.F_INFO, OrderItemHTable.INFO_C_COUNT,
					item.getCount().toString());
			HbaseOperateUtils.addStringCol(itemRow, OrderItemHTable.F_INFO, OrderItemHTable.INFO_C_GOO_NAME,
					item.getGooName());
			HbaseOperateUtils.addStringCol(itemRow, OrderItemHTable.F_INFO, OrderItemHTable.INFO_C_GOO_PRICE,
					item.getGooPrice().toString());
			itemRows.add(itemRow);
		});
		HbaseOperateUtils.addStringCol(orderRow, OrderHTable.F_DETAIL, OrderHTable.DETAIL_C_ITEM_IDS,
				StringUtils.collectionToCommaDelimitedString(itemIds));

		try {
			HbaseOperateUtils.getTable(orderTableN).put(orderRow);
			HbaseOperateUtils.getTable(orderItemTableN).put(itemRows);
			jedis.lpush(R_KEY_FOR_ORDER, orderCode);
		} catch (IOException e) {
			HbaseOperateUtils.deleteOne(orderTableN, orderCode);
			itemIds.stream().forEach(itemId -> HbaseOperateUtils.deleteOne(orderItemTableN, itemId));
			jedis.lrem(R_KEY_FOR_ORDER, 1, orderCode);
			e.printStackTrace();
		}
		return orderCode;
	}

	private String genOrderCode(String key) {
		long cur = System.currentTimeMillis();
		return new StringBuffer().append(key).append(cur).append(RandomStringUtils.randomAlphanumeric(4)).toString();
	}

	@Override
	public void updateStatus(String orderCode, int status) {
		Put row = new Put(Bytes.toBytes(orderCode));
		HbaseOperateUtils.addStringCol(row, OrderHTable.F_INFO, OrderHTable.INFO_C_STATUS, status + "");
		HbaseOperateUtils.printOne(orderTableN, orderCode);
	}

	@Override
	public void deleteOrder(String orderCode) {
		HbaseOperateUtils.deleteOne(orderTableN, orderCode);
		jedis.lrem(R_KEY_FOR_ORDER, 1, orderCode);
	}

	@Override
	public PageResult<OrderPo> queryPage(int page, int size) {
		/**
		 * 1 10 => 0 9 2 10 => 10 19
		 */
		List<String> rowkeys = jedis.lrange(R_KEY_FOR_ORDER, (page - 1) * size, page * size - 1);
		PageResult<OrderPo> result = new PageResult<>();
		result.setTotal(jedis.llen(R_KEY_FOR_ORDER));
		result.setDatas(convertResultToOrder(rowkeys));
		result.setPage(page);
		result.setSize(size);
		return result;
	}

	@Override
	public OrderPo getOrder(@Nonnull String orderCode) {
		OrderPo orderPo = new OrderPo();
		Result result = HbaseOperateUtils.getOne(orderTableN, orderCode);
		orderPo.setOrderCode(orderCode);
		orderPo.setFromUser(getValFromHResult(result, OrderHTable.F_INFO, OrderHTable.INFO_C_FROM_USER));
		orderPo.setToUser(getValFromHResult(result, OrderHTable.F_INFO, OrderHTable.INFO_C_TO_USER));
		orderPo.setStatus(getValFromHResult(result, OrderHTable.F_INFO, OrderHTable.INFO_C_STATUS));

		orderPo.setRemark(getValFromHResult(result, OrderHTable.F_DETAIL, OrderHTable.DETAIL_C_REMARK));
		Set<String> itemIds = StringUtils.commaDelimitedListToSet(
				getValFromHResult(result, OrderHTable.F_DETAIL, OrderHTable.DETAIL_C_ITEM_IDS));
		if (itemIds != null && !itemIds.isEmpty()) {
			orderPo.setItemIds(itemIds);
			orderPo.setItems(convertResultToItem(itemIds));
		}
		return orderPo;
	}

	private List<OrderPo> convertResultToOrder(List<String> orderCodes) {
		return orderCodes.stream().map(rowkey -> {
			return getOrder(rowkey);
		}).collect(Collectors.toList());
	}

	private List<OrderItemPo> convertResultToItem(Set<String> itemIds) {
		return itemIds.stream().map(itemId -> {
			OrderItemPo item = new OrderItemPo();
			Result result = HbaseOperateUtils.getOne(orderItemTableN, itemId);
			item.setCount(
					Integer.parseInt(getValFromHResult(result, OrderItemHTable.F_INFO, OrderItemHTable.INFO_C_COUNT)));
			item.setGooId(itemId);
			item.setGooName(getValFromHResult(result, OrderItemHTable.F_INFO, OrderItemHTable.INFO_C_GOO_NAME));
			item.setGooPrice(new BigDecimal(
					getValFromHResult(result, OrderItemHTable.F_INFO, OrderItemHTable.INFO_C_GOO_PRICE)));
			return item;
		}).collect(Collectors.toList());
	}

	private String getValFromHResult(Result result, String family, String name) {
		byte[] bytes = result.getValue(Bytes.toBytes(family), Bytes.toBytes(name));
		return bytes == null ? "" : new String(bytes);
	}

}
