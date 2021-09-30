package com.qijianguo.zookeeper.api;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 权限控制
 * @author qijianguo
 */
public class AuthInfo implements Watcher{

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private static ZooKeeper zk = null;

    private static String root = "/auth_info7";

    public static void main(String[] args) {
        try {
            System.out.println("-------------开始初始化-------------");
            zk = new ZooKeeper("127.0.0.1:2181", 5000, new AuthInfo());
            // 添加权限信息
            // scheme 权限模式：world、auth、digest、ip、super
            // auth 具体权限信息
            zk.addAuthInfo("digest", "xxxxxx4".getBytes());

            connectedSemaphore.await();
            // 检查节点是否存在
            Stat rootNode = zk.exists(root, false);
            if (rootNode == null) {
                // 创建根节点
                zk.create(root, "1213".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.EPHEMERAL);
                zk.create(root + "/c1", "1213".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.EPHEMERAL);
            }
            // 检验权限控制是否生效
            // 期望抛出异常信息：KeeperException$NoAuthException: KeeperErrorCode = NoAuth for /auth_info
            ZooKeeper zk2 = new ZooKeeper("127.0.0.1:2181", 5000, new AuthInfo());

            // 用无权限信息的客户端 删除有权限信息的客户端创建的 节点
            // 注意：是可以删除节点的（如果没有子结点的情况下），但是不能删除子结点
            //zk2.delete(root, -1);

            byte[] data = zk2.getData(root + "/c1", true, null);
            System.out.println(new String(data));

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
            }
        }
    }
}
