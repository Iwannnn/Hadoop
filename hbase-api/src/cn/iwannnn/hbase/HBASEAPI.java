package cn.iwannnn.hbase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class HBASEAPI {

	private Configuration conf = new Configuration();

	@Before
	public void initConf() {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.40.129:2181");
	}

	@Test
	public void testCreate() throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		TableName tableName = TableName.valueOf("student");
		HTableDescriptor table = new HTableDescriptor(tableName);
		HColumnDescriptor basic = new HColumnDescriptor("basic_info");
		HColumnDescriptor score = new HColumnDescriptor("score_info");
		table.addFamily(basic);
		table.addFamily(score);
		admin.createTable(table);
		admin.close();
	}

	@Test
	public void testPut() throws Exception {
		HTable table = new HTable(conf, "student");
		// Put put = new Put("zhangsan_001".getBytes());
		Put put = new Put(Bytes.toBytes("zhangsan_001"));
		put.add(Bytes.toBytes("basic_info"), Bytes.toBytes("id"), Bytes.toBytes("001"));
		put.add(Bytes.toBytes("basic_info"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
		put.add(Bytes.toBytes("basic_info"), Bytes.toBytes("age"), Bytes.toBytes("19"));
		put.add(Bytes.toBytes("basic_info"), Bytes.toBytes("gender"), Bytes.toBytes("man"));
		put.add(Bytes.toBytes("basic_info"), Bytes.toBytes("email"), Bytes.toBytes("zhangsan@qq.com"));
		put.add(Bytes.toBytes("score_info"), Bytes.toBytes("math"), Bytes.toBytes("90"));
		put.add(Bytes.toBytes("score_info"), Bytes.toBytes("english"), Bytes.toBytes("90"));
		put.add(Bytes.toBytes("score_info"), Bytes.toBytes("chinese"), Bytes.toBytes("90"));
		table.put(put);
		table.close();
	}

	@Test
	public void testPut10() throws Exception {
		long startTime = System.currentTimeMillis();
		HTable table = new HTable(conf, "student");
		// Put put = new Put("zhangsan_001".getBytes());
		List<Put> puts = new ArrayList<Put>();
		for (Integer i = 0; i <= 100000; i++) {
			Put put = new Put(Bytes.toBytes("index" + i.toString()));
			put.add(Bytes.toBytes("basic_info"), Bytes.toBytes("id"), Bytes.toBytes(i.toString()));
			puts.add(put);
		}
		table.put(puts);
		table.close();
		long endTime = System.currentTimeMillis();
		System.out.println(endTime - startTime + "ms");
	}

	@Test
	public void testGet() throws Exception {
		HTable table = new HTable(conf, "student");
		Get get = new Get(Bytes.toBytes("zhangsan_001"));
		Result res = table.get(get);
		NavigableMap<byte[], byte[]> familyMap = res.getFamilyMap(Bytes.toBytes("basic_info"));
		System.out.println(res);
		System.out.println(Bytes.toString(familyMap.get(Bytes.toBytes("id"))));
		byte[] values = res.getValue(Bytes.toBytes("basic_info"), Bytes.toBytes("name"));
		System.out.println(Bytes.toString(values));
		table.close();
	}

	@Test
	public void testScan() throws Exception {
		HTable table = new HTable(conf, "student");
		Scan scan = new Scan(Bytes.toBytes("zhangsan_001"));
		ResultScanner scanner = table.getScanner(scan);
		Iterator<Result> iterator = scanner.iterator();
		while (iterator.hasNext()) {
			Result result = iterator.next();
			byte[] value = result.getValue(Bytes.toBytes("basic_info"), Bytes.toBytes("name"));
			System.out.println(Bytes.toString(value));
		}
		table.close();
	}

	@Test
	public void testDel() throws Exception {
		HTable table = new HTable(conf, "student");
		Delete del = new Delete(Bytes.toBytes("lisi_002"));
		table.delete(del);
		table.close();
	}

	@Test
	public void testDelTable() throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		admin.disableTable("table02");
		admin.deleteTable("table02");
		admin.close();
	}
}
