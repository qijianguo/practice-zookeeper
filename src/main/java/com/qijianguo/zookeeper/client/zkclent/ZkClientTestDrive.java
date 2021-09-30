package com.qijianguo.zookeeper.client.zkclent;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 *
 * @author qijianguo
 */
public class ZkClientTestDrive {

    public static void main(String[] args) {
        ZkClient session = createSession();
        getChild(session);
    }

    public static ZkClient createSession() {
        // 创建会话
        ZkClient zkClient = new ZkClient("127.0.0.1:2181", 5000);

        IZkConnection con = new ZkConnection("127.0.0.1:2181", 5000);
        ZkSerializer zkSeri = new SerializableSerializer();
        ZkClient zk2 = new ZkClient(con, 500, zkSeri);

        return zkClient;
    }

    public static void createNode(ZkClient session) {
        // 创建节点 create(String, Object, CreateMode)
        // CreateMode 节点类型
        session.create("/zkclient/node/create", null, CreateMode.PERSISTENT);
        // 创建永久节点
        session.createPersistentSequential("/zkclient/persistent_node/create", null);
        // 创建临时节点
        session.createEphemeral("/zkclient/ephemeral_node/create", null);
        // 创建节点前先创建父节点（ZK api中如果父节点不存在，则不能创建子节点）
        session.createPersistent("/zkclient/ephemeral_node/create", true);

    }

    public static void deleteNode(ZkClient session) {
        // 删除节点
        session.delete("/zkclient/node/create");

        // 删除节点及其叶子节点（ZK api只允许删除叶子节点）
        session.deleteRecursive("/zkclient");

    }

    public static void getChild(ZkClient session) {
        session.delete("/zkclient");
        // 获取子结点
        // List<String> children = session.getChildren("/zkclient");
        // 获取子结点并监听节点
        List<String> handle_child_change = session.subscribeChildChanges("/zkclient", new IZkChildListener() {
            /**
             * 处理服务端发送的事件通知
             * 1.可以对不存在的节点进行监听
             * 2.节点本身的创建也会通知
             * 3.子结点列表的变化都会通知
             * 4.客户端只需要注册一次就可以了（不同于ZK api需要每次进行注册）
             * @param parentPath 子结点变化通知父节点的节点 绝对路径
             * @param currentChilds 子结点的 相对路径列表，可能为null
             * @throws Exception
             */
            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                System.out.println("handle child change：parentPath: " + parentPath + "  childs: "  + currentChilds);
            }
        });
        session.createPersistent("/zkclient");
        session.createPersistent("/zkclient/zk1", "111");
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        session.delete("/zkclient/zk1");
        // 取消事件注册
        // session.unsubscribeChildChanges();
        try {
            Thread.sleep(50000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void getData(ZkClient session) {
        // 读取数据，如果不存在节点，则抛出异常
        Object o = session.readData("/zkclient");
        // returnNullIfPathNotExists 如果节点不存在则会返回null， 不会抛出异常（ZK api中如果不存在则抛出异常NoNodeException）
        Object o1 = session.readData("/zkclient", true);
        // 传入旧的Stat，会被新的Stat替换
        Stat stat = new Stat();
        session.readData("zkclient", stat);
    }

    public static void setData(ZkClient session) {
        // 设置节点数据
        session.writeData("/zkclient", 111);
        // 修改制定版本的节点数据
        session.writeData("/zkclient", 111, 1);
    }

    public static void exists(ZkClient session) {
        boolean zkclient = session.exists("zkclient");
    }
}
