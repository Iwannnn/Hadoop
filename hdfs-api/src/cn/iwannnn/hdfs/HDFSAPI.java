package cn.iwannnn.hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class HDFSAPI {

	private Configuration conf;

	private FileSystem fileSystem;

	@Before
	public void init() throws Exception {
		conf = new Configuration();
		fileSystem = FileSystem.get(new URI("hdfs://192.168.40.129:9000"), conf, "root");
		conf.set("dfs.replication", "1");
		conf.addResource(new Path("E://env//hadoop-2.7.1//etc//hadoop//hdfssite.xml"));
	}

	@Test
	public void testConnect() throws IOException, InterruptedException, URISyntaxException {
		// 指定Hadoop配置,此处使用默认配置
		// 所有在xxx-site.xml文件的配置，都要设置
		conf = new Configuration();
		fileSystem = FileSystem.get(new URI("hdfs://192.168.40.129:9000"), conf, "root");
		// 连接文件系统 FileSystem Hadoop文件系统的抽象类
		// 基于http方式访问HDFS文件
		fileSystem.close();
	}

	@Test
	public void testGet() throws IOException, InterruptedException, URISyntaxException {
		// 指定下载文件 /rename.log
		InputStream in = fileSystem.open(new Path("/rename.log"));
		// 指定输出流
		FileOutputStream out = new FileOutputStream("rename.txt");
		// Hadoop 提供输入输出流工具
		IOUtils.copyBytes(in, out, conf);
		in.close();
		out.close();
		fileSystem.close();
	}

	@Test
	public void testCopytoLocal() throws IOException, InterruptedException, URISyntaxException {
		fileSystem.copyToLocalFile(new Path("/rename.log"), new Path("in.txt"));
		fileSystem.close();
	}

	@Test
	public void testUpload() throws IllegalArgumentException, IOException {
		FileInputStream in = new FileInputStream("E:/VS-Code/VS-Code-Hadoop/hdfs-api/upload.txt");
		FSDataOutputStream out = fileSystem.create(new Path("/upload.txt"));
		IOUtils.copyBytes(in, out, conf);
	}

	@Test
	public void testWrite() throws IllegalArgumentException, IOException {
		FileInputStream in = new FileInputStream("E:/VS-Code/VS-Code-Hadoop/hdfs-api/upload.txt");
		FSDataOutputStream out = fileSystem.create(new Path("/uploadwrite.txt"));
		byte[] b = new byte[1024];
		int len = 0;
		while ((len = in.read(b)) != -1) {
			out.write(b, 0, len);
		}
		in.close();
		out.close();
	}

	@Test
	public void testCopyFromLoacl() throws IOException {
		Path in = new Path("E:/VS-Code/VS-Code-Hadoop/hdfs-api/upload.txt");
		Path out = new Path("copyfromloacl.txt");
		fileSystem.copyFromLocalFile(in, out);
	}

	@Test
	public void testMkdir() throws IllegalArgumentException, IOException {
		fileSystem.mkdirs(new Path("/park"));
	}

	@Test
	public void testDel() throws IllegalArgumentException, IOException {
		fileSystem.delete(new Path("/uoloadapi.txt"));
	}

	@Test
	public void testLs() throws FileNotFoundException, IllegalArgumentException, IOException {
		FileStatus[] listStatus = fileSystem.listStatus(new Path("/"));
		for (FileStatus fileStatus : listStatus) {
			System.out.println(fileStatus);
		}
	}

	@Test
	public void testLsR() throws FileNotFoundException, IllegalArgumentException, IOException {
		RemoteIterator<LocatedFileStatus> listStatus = fileSystem.listFiles(new Path("/"), true);
		while (listStatus.hasNext()) {
			System.out.println(listStatus.next());
		}
	}

	@Test
	public void testRename() throws IllegalArgumentException, IOException {
		fileSystem.rename(new Path("/rename.log"), new Path("/rename.log"));
	}

	@Test
	public void modifyRep() throws IllegalArgumentException, IOException {
		fileSystem.setReplication(new Path("/data/score.txt"), (short) 1);
	}
}
