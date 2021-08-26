package cn.iwannnn.mstudent;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import lombok.Data;

@Data
public class Student implements Writable {

	private int month;

	private String name;

	private int score;

	@Override
	public void readFields(DataInput in) throws IOException {
		this.month = in.readInt();
		this.name = in.readUTF();
		this.score = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(month);
		out.writeUTF(name);
		out.writeInt(score);
	}

}
