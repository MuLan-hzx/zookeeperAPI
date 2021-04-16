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
 * @date 2021/4/15 17:14
 */
public class ZookeeperExists {
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
	 * 同步
	 * @throws Exception
	 */
	@Test
	public void exists1() throws Exception {
		Stat exists = zooKeeper.exists("/exists", false);
		System.out.println(exists.getVersion());
	}

	/**
	 * 异步
	 * @throws Exception
	 */
	@Test
	public void exists2() throws Exception {
		zooKeeper.exists("/exists", false, new AsyncCallback.StatCallback() {
			@Override
			public void processResult(int i, String s, Object o, Stat stat) {
				System.out.println(i);
				System.out.println(s);
				System.out.println(o);
				System.out.println(stat.getVersion());
			}
		},"i am context");
		Thread.sleep(10000);
		System.out.println("结束！");
	}

	/**
	 * 待写。。。
	 */
}
