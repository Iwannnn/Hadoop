package cn.iwannnn.hdfs.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

public class GSODHTable {

	private static Configuration conf;

	static {
		conf = HBaseConfiguration.create();
		// 指定zk实例
		conf.set("hbase.zookeeper.quorum", "hadoop");
	}

	public static void createTable() throws Exception {
		// 获取表管理器
		HBaseAdmin admin = new HBaseAdmin(conf);
		String tableName = GSODUtil.TABLE_NAME;
		byte[][] columnFamilies = GSODUtil.COLUMN_FAMILIES;
		TableName tName = TableName.valueOf(tableName);
		HTableDescriptor tableDescriptor = new HTableDescriptor(tName);
		for (byte[] cf : columnFamilies) {
			HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
			tableDescriptor.addFamily(columnDescriptor);
		}
		admin.createTable(tableDescriptor);
		admin.close();
	}

	public static void main(String[] args) throws Exception {
		createTable();
	}
}
