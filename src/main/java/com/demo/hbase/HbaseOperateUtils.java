package com.demo.hbase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public final class HbaseOperateUtils {

	private static Connection conn = null;
	public static final String ZOOKEEPER_CONF_FILE_NAME = "zookeeper-kn";
	public static final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	public static final String HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "hbase.zookeeper.property.clientPort";
	public static final String ZOOKEEPER_ZNODE_PARENT = "zookeeper.znode.parent";

	static {
		Configuration conf = new Configuration();
		ResourceBundle bundle = PropertyResourceBundle.getBundle(ZOOKEEPER_CONF_FILE_NAME);
		conf.set(HBASE_ZOOKEEPER_QUORUM, bundle.getString(HBASE_ZOOKEEPER_QUORUM));
		conf.set(HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, bundle.getString(HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT));
		if (!StringUtils.isEmpty(bundle.getString(ZOOKEEPER_ZNODE_PARENT))) {
			conf.set(ZOOKEEPER_ZNODE_PARENT, bundle.getString(ZOOKEEPER_ZNODE_PARENT));
		}
		try {
			conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			System.err.printf("连接获取失败:{%s} %n", e.getMessage());
		}
	}

	public static void createTableIfNotExits(List<String> familys, TableName tableName) throws IOException {
		if (familys == null || familys.size() <= 0) {
			return;
		}
		Set<String> familysSet = new HashSet<>(familys);
		Admin admin = conn.getAdmin();
		if (admin.tableExists(tableName)) {
			System.out.println(tableName + "表已经存在!");
		} else {
			HTableDescriptor desc = new HTableDescriptor(tableName);
			familysSet.stream().filter(a -> a != null).forEach(a -> desc.addFamily(new HColumnDescriptor(a)));
			admin.createTable(desc);
			System.out.println("创建表 \'" + tableName + "\' 成功!");
		}
	}

	public static void deleteTable(TableName tableName) {
		try {
			Admin admin = conn.getAdmin();
			if (!admin.tableExists(tableName)) {// 表不存在
				System.out.println(tableName + " is not exists!");
			} else {
				admin.disableTable(tableName);// 废弃表
				admin.deleteTable(tableName);// 删除表
				System.out.println(tableName + " is delete!");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Table getTable(TableName tableName) {
		try {
			return conn.getTable(tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void deleteOne(TableName tableName, String rowkey) {
		try {
			Table table = getTable(tableName);
			table.delete(new Delete(Bytes.toBytes(rowkey)));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Result getOne(TableName tableName, String rowkey) {
		try {
			Table table = getTable(tableName);
			return table.get(new Get(Bytes.toBytes(rowkey)));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;

	}

	/**
	 * 示例:
	 * <li>list.add("f1,col1,val1");</li>
	 * <li>list.add("f1,col2,val2");</li>
	 * 
	 * @param tableName
	 * @param conditions
	 * @author fuhw/vencano
	 */
	public static void selectByFilter(TableName tableName, List<String> conditions) {
		Table table = null;
		try {
			table = getTable(tableName);

			// 各个条件之间是“或”的关系，默认是“与”
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
			Scan scan = new Scan();
			for (String condition : conditions) {
				String[] s = condition.split(",");
				filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]),
						CompareOp.EQUAL, Bytes.toBytes(s[2])));
				// 添加下面这一行后，则只返回指定的cell，同一行中的其他cell不返回
				// s1.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
			}
			scan.setFilter(filterList);
			// ColumnPrefixFilter 用于指定列名前缀值相等
			// new ColumnPrefixFilter(Bytes.toBytes("values"));
			// s1.setFilter(f);

			// MultipleColumnPrefixFilter 和 ColumnPrefixFilter 行为差不多，但可以指定多个前缀
			// byte[][] prefixes = new byte[][] {Bytes.toBytes("value1"),
			// Bytes.toBytes("value2")};
			// Filter f = new MultipleColumnPrefixFilter(prefixes);
			// s1.setFilter(f);

			// QualifierFilter 是基于列名的过滤器。
			// Filter f = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new
			// BinaryComparator(Bytes.toBytes("col5")));
			// s1.setFilter(f);

			// RowFilter 是rowkey过滤器,通常根据rowkey来指定范围时，使用scan扫描器的StartRow和StopRow
			// 方法比较好。Rowkey也可以使用。
			// Filter f = new
			// RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new
			// RegexStringComparator(".*5$"));//正则获取结尾为5的行
			// s1.setFilter(f);
			ResultScanner resScan = table.getScanner(scan);
			resScan.forEach(result -> printNavigableMap(result));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (table != null) {
					table.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void printAll(TableName tableName) {
		try {
			Table table = getTable(tableName);
			Scan scan = new Scan();
			ResultScanner scanner = table.getScanner(scan);
			System.err.printf("查询表:[%s]的内容->", tableName);
			scanner.forEach(row -> {
				System.out.printf("\nRowkey: " + new String(row.getRow()));
				printNavigableMap(row);
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void printOne(TableName tableName, String rowkey) {
		try {
			Table table = getTable(tableName);
			System.err.printf("查询表:[%s],Rowkey=[%s]的内容为-> %n", tableName, rowkey);
			Result result = table.get(new Get(Bytes.toBytes(rowkey)));
			printNavigableMap(result);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void printNavigableMap(Result result) {
		NavigableMap<byte[], NavigableMap<byte[], byte[]>> noVersionMap = result.getNoVersionMap();
		noVersionMap.forEach((fl, da) -> {
			String family = new String(fl);
			Iterator<Entry<byte[], byte[]>> iterator = da.entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<byte[], byte[]> entry = iterator.next();
				System.out.printf("\t" + family + "." + new String(entry.getKey()));
				byte[] value = entry.getValue();
				System.out.printf(":" + (value == null ? "" : new String(value)));
			}
		});
	}

	public static void addStringCol(Put row, String colFamily, String colName, String colValue) {
		row.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colName), Bytes.toBytes(colValue));
	}
}
