package cn.iwannnn.mstudent;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class StudentPartitioner extends Partitioner<Text, Student> {

	@Override
	public int getPartition(Text key, Student value, int num) {
		return value.getMonth() - 1;
	}

}
