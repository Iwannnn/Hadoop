package cn.iwannnn.ip;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IPDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 获取当前默认配置
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		// 定义程序入口
		job.setJarByClass(cn.iwannnn.ip.IPDriver.class);
		// 定义Mapper类
		job.setMapperClass(cn.iwannnn.ip.IPMapper.class);
		// 定义Reducer类
		job.setReducerClass(cn.iwannnn.ip.IPReducer.class);
		// 定义最终输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BooleanWritable.class);
		//
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.40.129:9000/data/ip.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.40.129:9000/result/IP"));

		if (!job.waitForCompletion(true)) {
			return;
		}
	}
}
