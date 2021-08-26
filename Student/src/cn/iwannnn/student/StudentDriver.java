package cn.iwannnn.student;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StudentDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(cn.iwannnn.student.StudentDriver.class);
		job.setMapperClass(cn.iwannnn.student.StudentMapper.class);
		job.setReducerClass(cn.iwannnn.student.StudentReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Student.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Student.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.40.129:9000/data/score.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.40.129:9000/result/Student"));

		if (!job.waitForCompletion(true))
			return;
	}
}
