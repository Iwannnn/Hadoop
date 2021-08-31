package cn.iwannnn.hdfs.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GSODHdfsToHbase extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop");
		int exitCode = ToolRunner.run(conf, new GSODHdfsToHbase(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// 创建job对象
		Job job = Job.getInstance(getConf(), getClass().getSimpleName());
		// 类型
		job.setJarByClass(getClass());
		job.setMapperClass(HBaseDataMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(TableOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.40.129:9000/gsodRes/gsod.cvs"));
		// 输出
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, GSODUtil.TABLE_NAME);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	static class HBaseDataMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
		@Override
		public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
			String[] data = ivalue.toString().split(",");
			// STN+WBAN+DATE 作为rowkey 位于数组012位置 站点编号 海军空军编号 日期
			String rowKey = data[0] + data[1] + data[2];
			Put put = new Put(Bytes.toBytes(rowKey));

			// put.add(GSODUtil.COLUMN_FAMILIES[0], GSODUtil.INFO_STN,
			// Bytes.toBytes(data[0]));
			// put.add(GSODUtil.COLUMN_FAMILIES[0], GSODUtil.INFO_WBAN,
			// Bytes.toBytes(data[1]));
			// put.add(GSODUtil.COLUMN_FAMILIES[0], GSODUtil.INFO_DATE,
			// Bytes.toBytes(data[2]));

			// put.add(GSODUtil.COLUMN_FAMILIES[1], GSODUtil.DATA_TEMP,
			// Bytes.toBytes(data[3]));
			// put.add(GSODUtil.COLUMN_FAMILIES[1], GSODUtil.DATA_DEWP,
			// Bytes.toBytes(data[4]));
			// put.add(GSODUtil.COLUMN_FAMILIES[1], GSODUtil.DATA_SLP,
			// Bytes.toBytes(data[5]));
			// put.add(GSODUtil.COLUMN_FAMILIES[1], GSODUtil.DATA_STP,
			// Bytes.toBytes(data[6]));
			// put.add(GSODUtil.COLUMN_FAMILIES[1], GSODUtil.DATA_VISIB,
			// Bytes.toBytes(data[7]));
			// put.add(GSODUtil.COLUMN_FAMILIES[1], GSODUtil.DATA_WDSP,
			// Bytes.toBytes(data[8]));
			// put.add(GSODUtil.COLUMN_FAMILIES[1], GSODUtil.DATA_MXSPD,
			// Bytes.toBytes(data[9]));
			// put.add(GSODUtil.COLUMN_FAMILIES[1], GSODUtil.DATA_GUST,
			// Bytes.toBytes(data[10]));
			// put.add(GSODUtil.COLUMN_FAMILIES[1], GSODUtil.DATA_MAX,
			// Bytes.toBytes(data[11]));
			// put.add(GSODUtil.COLUMN_FAMILIES[1], GSODUtil.DATA_MIN,
			// Bytes.toBytes(data[12]));
			// put.add(GSODUtil.COLUMN_FAMILIES[1], GSODUtil.DATA_PRCP,
			// Bytes.toBytes(data[13]));
			// put.add(GSODUtil.COLUMN_FAMILIES[1], GSODUtil.DATA_SNDP,
			// Bytes.toBytes(data[14]));
			// put.add(GSODUtil.COLUMN_FAMILIES[1], GSODUtil.DATA_FOG,
			// Bytes.toBytes(data[15]));
			// put.add(GSODUtil.COLUMN_FAMILIES[1], GSODUtil.DATA_ROD,
			// Bytes.toBytes(data[16]));
			// put.add(GSODUtil.COLUMN_FAMILIES[1], GSODUtil.DATA_SOIP,
			// Bytes.toBytes(data[17]));
			// put.add(GSODUtil.COLUMN_FAMILIES[1], GSODUtil.DATA_HAIL,
			// Bytes.toBytes(data[18]));
			// put.add(GSODUtil.COLUMN_FAMILIES[1], GSODUtil.DATA_THUNDER,
			// Bytes.toBytes(data[19]));
			// put.add(GSODUtil.COLUMN_FAMILIES[1], GSODUtil.DATA_TORCF,
			// Bytes.toBytes(data[20]));

			// INFO
			for (int i = 0; i < 3; i++) {
				put.add(GSODUtil.COLUMN_FAMILIES[0], GSODUtil.COLUMNS[i], Bytes.toBytes(data[i]));
			}
			// DATA
			for (int i = 3; i < data.length; i++) {
				put.add(GSODUtil.COLUMN_FAMILIES[1], GSODUtil.COLUMNS[i], Bytes.toBytes(data[i]));
			}
			context.write(null, put);
		}
	}

}
