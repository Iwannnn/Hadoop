package cn.iwannnn.student;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StudentDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("E:/env/hadoop-2.7.1/etc/hadoop/hdfs-site.xml"));
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(cn.iwannnn.student.StudentDriver.class);
		job.setMapperClass(cn.iwannnn.student.StudentMapper.class);
		job.setReducerClass(cn.iwannnn.student.StudentReducer.class);

		job.setMapOutputKeyClass(Student.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Student.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.40.129:9000/data/score.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.40.129:9000/result/Student"));

		if (!job.waitForCompletion(true))
			return;
	}
}
