package cn.iwannnn.mprofit;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MProfitDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("E:/env/hadoop-2.7.1/etc/hadoop/hdfs-site.xml"));
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(cn.iwannnn.mprofit.MProfitDriver.class);
		job.setMapperClass(cn.iwannnn.mprofit.MProfitMapper.class);
		job.setPartitionerClass(cn.iwannnn.mprofit.MProfitPartitioner.class);
		job.setReducerClass(cn.iwannnn.mprofit.MProfitReducer.class);

		job.setNumReduceTasks(3);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MProfit.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MProfit.class);

		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.40.129:9000/data/profit.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.40.129:9000/result/Mprofit"));

		if (!job.waitForCompletion(true))
			return;
	}

}
