package cn.iwannnn.profit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import lombok.Data;

@Data
public class Profit implements Writable {

	private int month;

	private String name;

	private int in;

	private int out;

	private int profit;

	@Override
	public void readFields(DataInput input) throws IOException {
		this.month = input.readInt();
		this.name = input.readUTF();
		this.in = input.readInt();
		this.out = input.readInt();
		this.profit = this.in - this.out;
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(month);
		output.writeUTF(name);
		output.writeInt(in);
		output.writeInt(out);
		output.writeInt(profit);
	}

}
