package com.mulan.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author 韩志雄
 * @date 2021/4/14 0:56
 */
public class ZookeeperConnection {
	public static void main(String[] args) {


		try {
			//计数器对象
			CountDownLatch countDownLatch = new CountDownLatch(1);
			//arg1:Zookeeper服务器ip地址和端口号，
			//arg2:客户端与服务器的会话超时时间,单位毫秒
			//arg3:监视器对象
			ZooKeeper zooKeeper = new ZooKeeper("192.168.184.131:2181", 5000, new Watcher() {
				@Override
				public void process(WatchedEvent watchedEvent) {
					if(watchedEvent.getState()==Event.KeeperState.SyncConnected) {
						System.out.println("连接创建成功");
						countDownLatch.countDown();
					}
				}
			});
			//主线程阻塞等待连接对象创建成功
			countDownLatch.await();
			System.out.println(zooKeeper.getSessionId());
			zooKeeper.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
