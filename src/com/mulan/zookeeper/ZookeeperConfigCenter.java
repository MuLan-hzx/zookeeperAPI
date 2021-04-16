package com.mulan.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CountDownLatch;

/**
 * @author 韩志雄
 * @date 2021/4/15 22:35
 */
public class ZookeeperConfigCenter implements Watcher {

	private static final String CONNECTION_STRING = "192.168.184.131:2181";
	private static final int SESSION_TIMEOUT = 5000;
	private CountDownLatch countDownLatch = new CountDownLatch(1);
	private static ZooKeeper zooKeeper;

	private String url;
	private String name;
	private String password;

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public static void main(String[] args) {
		ZookeeperConfigCenter zookeeperConfigCenter = new ZookeeperConfigCenter();
		for (int i = 0; i < 30; i++) {
			try {
				Thread.sleep(5000);
				System.out.println("url:"+zookeeperConfigCenter.getUrl());
				System.out.println("name:"+zookeeperConfigCenter.getName());
				System.out.println("password:"+zookeeperConfigCenter.getPassword());
				System.out.println("--------------------------------------");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	public ZookeeperConfigCenter() {
		init();
	}

	//初始化读取配置文件
	private void init() {
		try {
			zooKeeper = new ZooKeeper(CONNECTION_STRING,SESSION_TIMEOUT,this);
			countDownLatch.await();
			this.url = new String(zooKeeper.getData("/config/url",true,null));
			this.name = new String(zooKeeper.getData("/config/name",true,null));
			this.password = new String(zooKeeper.getData("/config/password",true,null));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void process(WatchedEvent watchedEvent) {
		try {
			if (watchedEvent.getType() == Event.EventType.None) {
				if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
					System.out.println("连接成功！");
					countDownLatch.countDown();
				} else if (watchedEvent.getState() == Event.KeeperState.Disconnected) {
					System.out.println("连接断开！");
				} else if (watchedEvent.getState() == Event.KeeperState.Expired) {
					System.out.println("会话超时！");
					zooKeeper = new ZooKeeper(CONNECTION_STRING,SESSION_TIMEOUT,new ZookeeperConfigCenter());
				} else if (watchedEvent.getState() == Event.KeeperState.AuthFailed) {
					System.out.println("认证失败！");
				}
				//当配置文件发生改变时，重新读取配置文件
			} else if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {
				init();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
