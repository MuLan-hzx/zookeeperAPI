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
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author 韩志雄
 * @date 2021/4/15 16:50
 */
public class ZookeeperGetChildren {

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
	 * 异步
	 * @throws Exception
	 */
	@Test
	public void list1() throws Exception {

		zooKeeper.getChildren("/getChildren", true, new AsyncCallback.ChildrenCallback() {
			@Override
			public void processResult(int i, String s, Object o, List<String> list) {
				for (String str:list
					 ) {
					System.out.println(str);
				}
			}
		}, "i am context");
		Thread.sleep(10000);
		System.out.println("结束");
	}

	@Test
	public void getChildren2() throws Exception {
		List<String> children = zooKeeper.getChildren("/getChildren", false);
		for (String str : children) {
			System.out.println(str);
		}
	}
}
