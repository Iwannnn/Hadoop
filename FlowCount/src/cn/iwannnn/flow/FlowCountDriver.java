package cn.iwannnn.flow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowCountDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(cn.iwannnn.flow.FlowCountDriver.class);
		job.setMapperClass(cn.iwannnn.flow.FlowCountMapper.class);
		job.setReducerClass(cn.iwannnn.flow.FlowCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(cn.iwannnn.flow.UserFlow.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.40.129:9000/data/flow.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.40.129:9000/result/FlowCount"));

		if (!job.waitForCompletion(true))
			return;
	}
}
