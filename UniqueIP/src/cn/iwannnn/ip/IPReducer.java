package cn.iwannnn.ip;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IPReducer extends Reducer<Text, BooleanWritable, Text, Text> {
	public void reduce(Text keyin, Iterable<BooleanWritable> valuein, Context context)
			throws IOException, InterruptedException {
		context.write(keyin, new Text());
	}

}
