package cn.iwannnn.mstudent;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StudentMapper extends Mapper<LongWritable, Text, Text, Student> {
	public void map(LongWritable ikey, Text ivlaue, Context context) throws IOException, InterruptedException {
		String[] data = ivlaue.toString().split(" ");
		Student student = new Student();
		student.setMonth(Integer.parseInt(data[0]));
		student.setName(data[1]);
		student.setScore(Integer.parseInt(data[2]));
		context.write(new Text(student.getName()), student);
	}
}
