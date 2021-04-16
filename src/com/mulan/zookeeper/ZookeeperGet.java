package com.mulan.zookeeper;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author 韩志雄
 * @date 2021/4/15 16:37
 */
public class ZookeeperGet {

	ZooKeeper zooKeeper;

	private static final String CONNECTION_STRING = "192.168.184.131:2181";
	private static final int SESSION_TIMEOUT = 5000;

	//创建Zookeeper对象
	@Before
	public void pre() throws IOException, InterruptedException {
		CountDownLatch countDownLatch = new CountDownLatch(1);
		zooKeeper = new ZooKeeper(CONNECTION_STRING, SESSION_TIMEOUT, new Watcher() {
			@Override
			public void process(WatchedEvent watchedEvent) {
				if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
					System.out.println("连接成功！");
					countDownLatch.countDown();
				}
			}
		});

		System.out.println("当前会话ID:"+ zooKeeper.getSessionId());
		countDownLatch.await();
	}

	//关闭会话
	@After
	public void after() throws Exception {
		zooKeeper.close();
	}

	/**
	 * 同步get
	 */
	@Test
	public void get1() throws Exception {
		//创建状态对象
		Stat stat = new Stat();
		//watch :使用连接对象中注册的监听器
		byte[] data = zooKeeper.getData("/get/node1", true, stat);
		System.out.println(stat.getVersion());
		System.out.println(new String(data));
	}

	/**
	 * 异步get
	 */
	@Test
	public void get2() throws Exception {
		//watch :使用连接对象中注册的监听器
		zooKeeper.getData("/get/node1", false, (i, s, o, bytes, stat) -> {
			System.out.println(i);
			System.out.println(s);
			System.out.println(o);
			System.out.println(stat.getAversion());
		},"i am context");
		Thread.sleep(10000);
		System.out.println("结束!");
	}
}
