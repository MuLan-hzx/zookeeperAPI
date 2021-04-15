package com.mulan.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author 韩志雄
 * @date 2021/4/14 12:25
 */
public class ZookeeperCreate {

	//连接字符串：Zookeeper服务器ip地址和端口号
	public final String CONNECT_STRING = "192.168.184.131:2181";
	//客户端与服务器的会话超时时间,单位毫秒
	public final int SESSION_TIMEOUT=5000;
	ZooKeeper zooKeeper;

	@Before
	public void pre() throws Exception{
		//计数器对象(JUC的)
		CountDownLatch countDownLatch = new CountDownLatch(1);
		//arg1:Zookeeper服务器ip地址和端口号，
		//arg2:客户端与服务器的会话超时时间,单位毫秒
		//arg3:监视器对象
		zooKeeper = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, watchedEvent -> {
			if(watchedEvent.getState()== Watcher.Event.KeeperState.SyncConnected) {
				System.out.println("连接创建成功");
				countDownLatch.countDown();
			}
		});
		//主线程阻塞等待连接对象创建成功
		countDownLatch.await();
		System.out.println("会话ID:"+zooKeeper.getSessionId());
	}

	@After
	public void after() throws InterruptedException {
		zooKeeper.close();
	}

	@Test
	public void create1 () throws InterruptedException, KeeperException {

		/**
		 * arg1:节点的路径
		 * arg2:节点数据的二进制形式
		 * arg3：权限列表 world:anyone:cdrwa
		 * arg4：节点类型-->持久化
		 */
		zooKeeper.create("/create/node1","node1".getBytes(),
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	@Test
	public void create2() throws InterruptedException, KeeperException {
		//world:anyone:r
		zooKeeper.create("/create2/node2","node2".getBytes(),
				ZooDefs.Ids.READ_ACL_UNSAFE,CreateMode.PERSISTENT);
	}

	/**
	 * Schema:world	id:anyone
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	@Test
	public void create3() throws InterruptedException, KeeperException {
		//world授权模式
		List<ACL> acls = new ArrayList<>();
		//授权模式和授权对象
		Id id = new Id("world","anyone");
		acls.add(new ACL(ZooDefs.Perms.READ,id));
		acls.add(new ACL(ZooDefs.Perms.CREATE,id));
		zooKeeper.create("/create/node3","node3".getBytes(),
				acls,CreateMode.PERSISTENT);
	}

	/**
	 * schema:ip id:192.168.xxx.xxx
	 */
	@Test
	public void create4() throws InterruptedException, KeeperException {
		List<ACL> acls = new ArrayList<>();
		Id id = new Id("ip","192.168.184.131");
		acls.add(new ACL(ZooDefs.Perms.ALL,id));
		zooKeeper.create("/create/node4","node4".getBytes(),
				acls,CreateMode.PERSISTENT);
	}

	/**
	 * auth授权模式，添加授权用户！
	 * @throws Exception
	 */
	@Test
	public void create5() throws Exception {
		zooKeeper.addAuthInfo("digest","mulan:123456".getBytes());
		zooKeeper.create("/create/node5","node5".getBytes(),ZooDefs.Ids.CREATOR_ALL_ACL,CreateMode.PERSISTENT);
	}

	@Test
	public void create6() throws Exception {
		//auth授权模式，添加权限
		zooKeeper.addAuthInfo("digest","mulan:123456".getBytes());
		//创建访问控制表列表
		List<ACL> acls = new ArrayList<>();
		//创建授权模式和授权对象,大id
		Id id = new Id("auth","mulan");
		//权限：读,创建访问控制表加入集合
		acls.add(new ACL(ZooDefs.Perms.READ,id));
		//创建节点,添加二进制数据,
		zooKeeper.create("/create/node6","node6".getBytes(),
				acls,CreateMode.PERSISTENT);
	}

	/**
	 * digest授权模式
	 * @throws Exception
	 */
	@Test
	public void create7() throws Exception {

		//创建访问控制表列表
		List<ACL> acls = new ArrayList<>();
		//创建授权模式和授权对象,大id
		Id id = new Id("digest","mulan:xxxxxxxxxxx");
		acls.add(new ACL(ZooDefs.Perms.ALL,id));
		zooKeeper.create("/create/node7","node7".getBytes(),
				acls,CreateMode.PERSISTENT);
	}

	/**
	 * 持久化顺序节点
	 * @throws Exception
	 */
	@Test
	public void create8() throws Exception {

		String s = zooKeeper.create("/create/node8",
				"node8".getBytes(),
				ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT_SEQUENTIAL);
		System.out.println(s);
	}

	/**
	 *临时节点
	 * @throws Exception
	 */
	@Test
	public void create9() throws Exception {

		String s = zooKeeper.create("/create/node9",
				"node9".getBytes(),
				ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL);
		System.out.println(s);
	}

	/**
	 * 临时顺序节点
	 * @throws Exception
	 */
	@Test
	public void create10() throws Exception {

		String s = zooKeeper.create("/create/node10",
				"node10".getBytes(),
				ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(s);
	}

	/**
	 * 异步方式创建节点
	 * @throws Exception
	 */
	@Test
	public void create11() throws Exception {
		zooKeeper.create("/create/node11",
				"node11".getBytes(),
				ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT,
				(i, s, o, s1) -> {
					//0代表创建成功
					System.out.println("rc:"+i);
					//节点路径
					System.out.println("节点路径："+s);
					//上下文对象
					System.out.println("上下文对象："+o);
					//上下文参数
					System.out.println("上下文参数："+s1);
				},
		"I am context");
		Thread.sleep(10000);
		System.out.println("结束");
	}
}
