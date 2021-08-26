package cn.iwannnn.profit;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProfitPartitioner extends Partitioner<Text, Profit> {

	public static Map<String, Integer> values = new HashMap<>();;

	public static int index = 0;

	@Override
	public int getPartition(Text arg0, Profit arg1, int arg2) {
		String name = arg1.getName();
		if (values.get(name) == null) {
			values.put(name, index++);
			return values.get(name);
		} else {
			return values.get(name);
		}
	}

}
