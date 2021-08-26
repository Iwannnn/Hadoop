package cn.iwannnn.student;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StudentReducer extends Reducer<Text, Student, Text, Student> {
	public void reduce(Text ikey, Iterable<Student> ivalues, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (Student stu : ivalues) {
			sum = stu.getChinese() + stu.getEngilsh() + stu.getMath();
			stu.setTotal(sum);
			context.write(ikey, stu);
		}
	}

}
