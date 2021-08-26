package cn.iwannnn.profit;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfitMapper extends Mapper<LongWritable, Text, Text, Profit> {
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String[] data = ivalue.toString().split(" ");
		Profit profit = new Profit();
		profit.setMonth(Integer.parseInt(data[0]));
		profit.setName(data[1]);
		profit.setIn(Integer.parseInt(data[2]));
		profit.setOut(Integer.parseInt(data[3]));
		context.write(new Text(profit.getName()), profit);
	}
}
