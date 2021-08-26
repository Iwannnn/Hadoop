package cn.iwannnn.mstudent;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StudentReducer extends Reducer<Text, Student, Text, IntWritable> {
	public void reduce(Text ikey, Iterable<Student> ivalues, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (Student stu : ivalues) {
			sum += stu.getScore();
		}
		context.write(ikey, new IntWritable(sum));
	}

}
