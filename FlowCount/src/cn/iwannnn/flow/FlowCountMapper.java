package cn.iwannnn.flow;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, UserFlow> {
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String[] data = ivalue.toString().split(" ");
		UserFlow userFlow = new UserFlow();
		userFlow.setPhone(data[0]);
		userFlow.setAddr(data[1]);
		userFlow.setUsername(data[2]);
		userFlow.setFlow(Integer.parseInt(data[3]));
		context.write(new Text(userFlow.getUsername()), userFlow);
	}

}
