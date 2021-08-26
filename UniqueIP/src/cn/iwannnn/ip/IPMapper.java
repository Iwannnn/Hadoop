package cn.iwannnn.ip;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IPMapper extends Mapper<LongWritable, Text, Text, BooleanWritable> {

	public void map(LongWritable keyin, Text valuein, Context context) throws IOException, InterruptedException {
		String ip = valuein.toString();
		context.write(new Text(ip), new BooleanWritable(true));

	}

}
