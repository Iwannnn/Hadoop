package cn.iwannnn.gsod;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GSODMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	// 按行读取
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();
		if (line.startsWith("id")) {
			return;
		}
		if (line.equals("")) {
			return;
		}
		String[] data = line.split(",");

		String newData = "";
		if (!data[3].equals("CH")) {
			return;
		}
		data[5] = data[5] == "9999.9" ? "0" : data[5];
		data[7] = data[7] == "9999.9" ? "0" : data[7];
		data[8] = data[8] == "9999.9" ? "0" : data[8];
		data[9] = data[9] == "999.9" ? "0" : data[9];
		data[10] = data[10] == "999.9" ? "0" : data[10];
		data[11] = data[11] == "999.9" ? "0" : data[11];
		data[12] = data[12] == "9999.9" ? "0" : data[12];
		data[13] = data[13] == "9999.9" ? "0" : data[13];
		data[14] = data[14] == "9999.99" ? "0" : data[14];
		data[15] = data[15] == "99.99" ? "0" : data[15];
		data[16] = data[16] == "999.99" ? "0" : data[16];
		for (int i = 0; i < data.length; i++) {
			if (i == 0 || i == 3) {
				continue;
			} else {
				if (i != data.length - 1) {
					newData += data[i] + ",";
				} else {
					newData += data[i];
				}
			}
		}
		context.write(new Text(newData), NullWritable.get());
	}

}
