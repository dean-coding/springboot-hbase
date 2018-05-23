package com.demo.hase;

import java.util.Arrays;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.demo.hbase.HbaseOperateUtils;

public class HbaseTest {

	public static void main(String[] args) throws Exception {

		String family1 = "info";
		String family2 = "detail";
		TableName tableName = TableName.valueOf("sys_user");
		HbaseOperateUtils.createTableIfNotExits(Arrays.asList(family1, family2), tableName);

		// 2.添加数据到表
		Table table = HbaseOperateUtils.getTable(tableName);
		String randomNumeric = RandomStringUtils.randomNumeric(2);
		Put colOne = new Put(Bytes.toBytes(RandomStringUtils.randomAlphabetic(8)));//
		HbaseOperateUtils.addStringCol(colOne, family1, "name", "xiaolang" + randomNumeric);
		HbaseOperateUtils.addStringCol(colOne, family1, "age", "26" + randomNumeric);
		HbaseOperateUtils.addStringCol(colOne, family2, "address", "beijing.chaoyang" + randomNumeric);
		table.put(Arrays.asList(colOne));

		// 3.查询表内容:
		HbaseOperateUtils.printAll(tableName);
	}

}
