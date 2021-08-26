package cn.iwannnn.chars;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CharCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text keyin, Iterable<IntWritable> valuein, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : valuein) {
			sum += val.get();
		}
		context.write(keyin, new IntWritable(sum));
	}

}
