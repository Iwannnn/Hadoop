package cn.iwannnn.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowCountPartitioner extends Partitioner<Text, UserFlow> {

	@Override
	public int getPartition(Text key, UserFlow value, int num) {
		String addr = value.getAddr();
		if ("bj".equals(addr)) {
			return 0;
		} else if ("sh".equals(addr)) {
			return 1;
		} else if ("sz".equals(addr)) {
			return 2;
		}
		return 3;
	}

}
