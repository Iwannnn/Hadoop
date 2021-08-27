package cn.iwannnn.student;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import lombok.Data;

@Data
public class Student implements WritableComparable<Student> {

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
		this.total = this.chinese + this.engilsh + this.math;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeInt(chinese);
		out.writeInt(engilsh);
		out.writeInt(math);
		out.writeInt(total);
	}

	@Override
	public int compareTo(Student o) {
		return -1 * (this.total - o.total == 0 ? this.chinese - o.chinese == 0
				? this.engilsh - o.engilsh == 0 ? this.math - o.math : this.engilsh - o.engilsh
				: this.chinese - o.chinese : this.total - o.total);
	}

}
