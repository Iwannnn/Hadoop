package cn.iwannnn.chars;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CharCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable keyin, Text valuein, Context context) throws IOException, InterruptedException {
		String line = valuein.toString();
		String[] chars = line.split("");
		for (String ch : chars) {
			context.write(new Text(ch), new IntWritable(1));
		}

	}

}
