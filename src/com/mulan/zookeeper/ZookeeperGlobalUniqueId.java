package com.mulan.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author 韩志雄
 * @date 2021/4/15 23:42
 */
public class ZookeeperGlobalUniqueId implements Watcher {

	//连接字符串：Zookeeper服务器ip地址和端口号
	public static final String CONNECT_STRING = "192.168.184.131:2181";
	//客户端与服务器的会话超时时间,单位毫秒
	public static final int SESSION_TIMEOUT=5000;
	private static CountDownLatch countDownLatch = new CountDownLatch(1);
	private static ZooKeeper zooKeeper;
	private String defaultPath = "/uniqueId";

	@Override
	public void process(WatchedEvent watchedEvent) {
		//事件类型
		if (watchedEvent.getType() == Event.EventType.None) {
			if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
				System.out.println("连接创建成功！");
				countDownLatch.countDown();
			} else if (watchedEvent.getState() == Event.KeeperState.Disconnected) {
				System.out.println("断开连接！");
			}else if (watchedEvent.getState() == Event.KeeperState.Expired) {
				System.out.println("会话超时！");
				try {
					zooKeeper = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, this);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}else if (watchedEvent.getState() == Event.KeeperState.AuthFailed) {
				System.out.println("认证失败！");
			}
		}
	}

	public ZookeeperGlobalUniqueId() {

		try {
			zooKeeper = new ZooKeeper(CONNECT_STRING,SESSION_TIMEOUT,this);
			countDownLatch.await();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//生成ID的方法
	public String getUniqueId() {
		String  path = "";
		try {
			//创建临时有序变量
			path = zooKeeper.create(defaultPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return path.substring(9);
	}

	public static void main(String[] args) {
		ZookeeperGlobalUniqueId globalUniqueId = new ZookeeperGlobalUniqueId();
		String uniqueId = null;
		for (int i = 0; i < 5; i++) {
			try {
				uniqueId = globalUniqueId.getUniqueId();
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println(uniqueId);
		}
	}
}
