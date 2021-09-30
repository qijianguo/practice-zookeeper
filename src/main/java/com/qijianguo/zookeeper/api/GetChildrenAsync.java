package com.qijianguo.zookeeper.api;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 异步读取数据
 * 节点的获取异步化：例如应用启动时配置信息的获取，异步获取可以避免影响应用的主流程
 * @author qijianguo
 */
public class GetChildrenAsync {

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private static ZooKeeper zk = null;

    private static String root = "/get_children_async";

    public static void main(String[] args) {
        try {
            zk = new ZooKeeper("127.0.0.1:2181", 5000, new ZkWatcher());
            connectedSemaphore.await();
            // 创建根节点
            zk.create(root, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create(root + "/c1", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            // 异步获取子节点
            zk.getChildren(root, true, new IChildrenCallback(), null);

            /**
             * 添加第二个子结点，观察 {@link ZkWatcher#process(WatchedEvent) 中回调函数的变化}
             */
            zk.create(root + "/c2", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            // 添加第三个子节点
            zk.create(root + "/c3", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

            Thread.sleep(Integer.MAX_VALUE);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }

    }

    /*

        输出结果:
        0,/get_children_async,null,[c1],18,18,1631687105201,1631687105201,0,1,0,0,0,1,19

        ReGetChildren:[c3, c1, c2]

    */

    static class ZkWatcher implements Watcher {

        public void process(WatchedEvent event) {
            if (Event.KeeperState.SyncConnected == event.getState()) {
                if (Event.EventType.None == event.getType() && null == event.getPath()) {
                    connectedSemaphore.countDown();
                } else if (Event.EventType.NodeChildrenChanged == event.getType()) {
                    try {
                        // 事件通知只包含通知类型，具体变化需要客户端自己重新获取
                        System.out.println("ReGetChildren:" + zk.getChildren(event.getPath(), true));
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }

        }
    }

    static class IChildrenCallback implements AsyncCallback.Children2Callback {

        public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
            System.out.println(rc + "," + path + "," + ctx + "," + children + "," + stat );
        }
    }

}
