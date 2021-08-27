package cn.iwannnn.student;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class StudentReducer extends Reducer<Student, NullWritable, Student, NullWritable> {
	public void reduce(Student ikey, NullWritable ivalues, Context context) throws IOException, InterruptedException {
		context.write(ikey, NullWritable.get());
	}

}
