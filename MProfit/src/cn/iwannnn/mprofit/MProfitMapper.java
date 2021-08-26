package cn.iwannnn.mprofit;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MProfitMapper extends Mapper<LongWritable, Text, Text, MProfit> {
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String[] data = ivalue.toString().split(" ");
		MProfit profit = new MProfit();
		profit.setMonth(Integer.parseInt(data[0]));
		profit.setName(data[1]);
		profit.setIn(Integer.parseInt(data[2]));
		profit.setOut(Integer.parseInt(data[3]));
		context.write(new Text(profit.getName()), profit);
	}
}
