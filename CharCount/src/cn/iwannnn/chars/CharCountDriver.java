package cn.iwannnn.chars;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CharCountDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 获取当前默认配置
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		// 定义程序入口
		job.setJarByClass(cn.iwannnn.chars.CharCountDriver.class);
		// 定义Mapper类
		job.setMapperClass(cn.iwannnn.chars.CharCountMapper.class);
		// 定义Reducer类
		job.setReducerClass(cn.iwannnn.chars.CharCountReducer.class);
		// 定义最终输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.40.129:9000/data/characters.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.40.129:9000/result/CharCount"));

		if (!job.waitForCompletion(true)) {
			return;
		}
	}
}
