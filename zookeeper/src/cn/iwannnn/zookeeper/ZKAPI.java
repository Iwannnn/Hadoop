package cn.iwannnn.zookeeper;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

public class ZKAPI {

	ZooKeeper zk;

	@Before
	public void testConnect() throws Exception {
		CountDownLatch cdl = new CountDownLatch(1);
		zk = new ZooKeeper("192.168.40.129:2182,192.168.40.129:2183,192.168.40.129:2184", 5000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				if (event.getState() == KeeperState.SyncConnected) {
					System.out.println("success");
				}
				cdl.countDown();
			}
		});
		cdl.await();
		if (zk == null) {
			throw new RuntimeException("zk null");
		}
	}

	@Test
	public void testCreate() throws Exception {
		String node03 = zk.create("/node03", "api test".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println(node03);
	}

	@Test
	public void testDelete() throws Exception {
		zk.delete("/node03", 0);
	}

	@Test
	public void testSetData() throws Exception {
		zk.setData("/node03", "setData".getBytes(), 0);
	}

	@Test
	public void tsetGetData() throws Exception {
		String data = zk.getData("/node03", true, new Stat()).toString();
		System.out.println(data);
	}

	@Test
	public void testGetChile() throws Exception {
		List<String> children = zk.getChildren("/node01", true);
		System.out.println(children.toString());

		// for (String child : children) {
		// String data = zk.getData(child, true, new Stat()).toString();
		// System.out.println(data);
		// }
	}
}
