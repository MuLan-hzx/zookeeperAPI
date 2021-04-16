package com.mulan.zookeeper.watcher;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author 韩志雄
 * @date 2021/4/15 17:24
 */
public class ZookeeperConnectionWatcher implements Watcher {

	//连接字符串：Zookeeper服务器ip地址和端口号
	public static final String CONNECT_STRING = "192.168.184.131:2181";
	//客户端与服务器的会话超时时间,单位毫秒
	public static final int SESSION_TIMEOUT=5000;
	private static CountDownLatch countDownLatch = new CountDownLatch(1);
	private static ZooKeeper zooKeeper;

	public static void main(String[] args) throws Exception {
		zooKeeper = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, watchedEvent -> {
			if(watchedEvent.getState()== Watcher.Event.KeeperState.SyncConnected) {
				System.out.println("连接创建成功");
				countDownLatch.countDown();
			}
		});
		//主线程阻塞等待连接对象创建成功
		countDownLatch.await();
		System.out.println("会话ID:"+zooKeeper.getSessionId());
		//添加授权用户
		zooKeeper.addAuthInfo("digest","mulan:123456".getBytes());
		byte[] data = zooKeeper.getData("/create", false, null);
		System.out.println(new String(data));
	}
	@Override
	public void process(WatchedEvent watchedEvent) {
		//事件类型
		if (watchedEvent.getType() == Event.EventType.None) {
			if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
				System.out.println("连接创建成功！");
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
}
