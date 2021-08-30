package cn.iwannnn.gsod;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GSODDricer {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("E:/env/hadoop-2.7.1/etc/hadoop/hdfs-site.xml"));
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(cn.iwannnn.gsod.GSODDricer.class);
		job.setMapperClass(cn.iwannnn.gsod.GSODMapper.class);
		// job.setReducerClass(cn.iwannnn.gsod.GSODReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop:9000/gsodData"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop:9000/result/gsod"));

		if (!job.waitForCompletion(true))
			return;
	}

}
