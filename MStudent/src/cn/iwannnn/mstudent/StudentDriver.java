package cn.iwannnn.mstudent;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StudentDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("E:/env/hadoop-2.7.1/etc/hadoop/hdfs-site.xml"));
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(cn.iwannnn.mstudent.StudentDriver.class);
		job.setMapperClass(cn.iwannnn.mstudent.StudentMapper.class);
		job.setPartitionerClass(cn.iwannnn.mstudent.StudentPartitioner.class);
		job.setReducerClass(cn.iwannnn.mstudent.StudentReducer.class);

		job.setNumReduceTasks(3);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Student.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.40.129:9000/data/score1"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.40.129:9000/result/score"));

		if (!job.waitForCompletion(true))
			return;
	}
}
