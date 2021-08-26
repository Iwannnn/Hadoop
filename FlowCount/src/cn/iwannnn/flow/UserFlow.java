package cn.iwannnn.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import lombok.Data;

@Data
public class UserFlow implements Writable {

	private String phone;
	private String addr;
	private String username;
	private int flow;

	// 序列化方法：在其中将属性值依次输出，需要使用匹配的类型输出方法
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(phone);
		out.writeUTF(addr);
		out.writeUTF(username);
		out.writeInt(flow);
	}

	// 反序列化方法：在其中将in流中的属性值依次读取并赋值给对象相应属性
	@Override
	public void readFields(DataInput in) throws IOException {
		this.phone = in.readUTF();
		this.addr = in.readUTF();
		this.username = in.readUTF();
		this.flow = in.readInt();
	}

}
