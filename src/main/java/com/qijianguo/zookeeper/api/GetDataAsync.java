package com.qijianguo.zookeeper.api;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 异步获取数据
 * @author qijianguo
 */
public class GetDataAsync implements Watcher, AsyncCallback.DataCallback {

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private static ZooKeeper zk = null;

    private static String root = "/get_data_async7";

    public static void main(String[] args) {
        try {
            System.out.println("-------------开始初始化-------------");
            zk = new ZooKeeper("127.0.0.1:2181", 5000, new GetDataAsync());
            connectedSemaphore.await();
            // 创建根节点
            zk.create(root, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create(root + "/c1", "I‘m c1.".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            zk.getData(root + "/c1", true, new GetDataAsync(), null);
            // 更新节点数据, -1表示基于最新版本号进行操作，适用于没有原子性要求的场景
            // 返回节点状态信息，包含事务ID、版本号、modifyData等。
            Stat stat = zk.setData(root + "/c1", "new Data...".getBytes(), -1);
            zk.setData(root + "/c1", "new Data...".getBytes(), stat.getVersion());
            // 由于此时version = 2了，而stat.getVersion = 1, 则会修改失败-KeeperException$BadVersionException
            zk.setData(root + "/c1", "new Data...".getBytes(), stat.getVersion());
            System.out.println("update: version:" + stat.getVersion() + " - czxid:" + stat.getCzxid());
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

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            if (Event.EventType.None == event.getType()) {
                connectedSemaphore.countDown();
            } else if (Event.EventType.NodeDataChanged == event.getType()) {
                zk.getData(event.getPath(), true, new GetDataAsync(), null);
            }

        }
    }

    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        System.out.println("async：" + new String(data) + " version:" + stat.getVersion());
    }
}
