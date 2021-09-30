package com.qijianguo.zookeeper.api;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class GetDataSync implements Watcher {

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private static ZooKeeper zk = null;

    /**
     * 指定节点的状态信息
     * 方法ZooKeeper#getData(String, boolean, Stat)中传入一个旧的状态信息，调用过程中被服务端响应的新的stat替换掉。
     */
    private static Stat stat = new Stat();

    private static String root = "/get_data_sync11";

    public static void main(String[] args) {
        try {
            System.out.println("-------------开始初始化-------------");
            zk = new ZooKeeper("127.0.0.1:2181", 5000, new GetDataSync());
            connectedSemaphore.await();
            // 创建根节点
            zk.create(root, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create(root + "/c1", "I‘m c1.".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            byte[] data = zk.getData(root + "/c1", true, stat);
            System.out.println("init:" + new String(data) + " version:" + stat.getVersion());
            zk.setData(root + "/c1", "new Data...".getBytes(), -1);
            zk.setData(root + "/c1", "new Data2...".getBytes(), -1);
            System.out.println("-------------初始化完毕-------------");
            Thread.sleep(Integer.MAX_VALUE);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }

    }

    /**
     * Watcher事件
     * @param event
     */
    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            if (Event.EventType.None == event.getType()) {
                connectedSemaphore.countDown();
            } else if (Event.EventType.NodeDataChanged == event.getType()) {
                try {
                    byte[] data = zk.getData(event.getPath(), true, stat);
                    System.out.println("changed:" + new String(data) + " version:" + stat.getVersion());
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }

    }
}
