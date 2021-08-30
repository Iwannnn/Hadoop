package cn.iwannnn.gsod;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.net.URI;

public class GSODRename {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("E:/env/hadoop-2.7.1/etc/hadoop/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.40.129:9000"), conf, "root");
		fs.rename(new Path("hdfs://192.168.40.129:9000/result/gsod/part-r-00000"),
				new Path("hdfs://192.168.40.129:9000/result/gsod/gsodres"));
	}
}
