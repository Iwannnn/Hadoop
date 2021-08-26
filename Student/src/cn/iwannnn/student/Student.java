package cn.iwannnn.student;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import lombok.Data;

@Data
public class Student implements Writable {

	private String name;

	private int chinese;

	private int engilsh;

	private int math;

	private int total;

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.chinese = in.readInt();
		this.engilsh = in.readInt();
		this.math = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeInt(chinese);
		out.writeInt(engilsh);
		out.writeInt(math);
	}

}
