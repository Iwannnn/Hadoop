package cn.iwannnn.student;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StudentMapper extends Mapper<LongWritable, Text, Student, NullWritable> {
	public void map(LongWritable ikey, Text ivlaue, Context context) throws IOException, InterruptedException {
		String[] data = ivlaue.toString().split(" ");
		Student student = new Student();
		student.setName(data[0]);
		student.setChinese(Integer.parseInt(data[1]));
		student.setEngilsh(Integer.parseInt(data[2]));
		student.setMath(Integer.parseInt(data[3]));
		student.setTotal(student.getChinese() + student.getEngilsh() + student.getMath());
		context.write(student, NullWritable.get());
	}
}
