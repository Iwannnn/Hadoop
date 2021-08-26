package cn.iwannnn.flow;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowCountReducer extends Reducer<Text, UserFlow, Text, IntWritable> {
	public void reduce(Text ikey, Iterable<UserFlow> ivalues, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (UserFlow val : ivalues) {
			sum += val.getFlow();
		}
		context.write(ikey, new IntWritable(sum));
	}

}
