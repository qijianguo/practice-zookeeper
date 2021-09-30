package com.qijianguo.zookeeper.client.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.CreateMode;

public class CuratorTestDrive {

    public static void main(String[] args) {

    }

    /**
     * 创建会话并启动
     * @return CuratorFramework
     */
    private static CuratorFramework createSession() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

        /**
         * 通过 {@link CuratorFrameworkFactory#newClient(String, int, int, RetryPolicy)} 创建客户端
         * retryPolicy 重试策略, 可以自定义
         */
        CuratorFramework session = CuratorFrameworkFactory.newClient(
                "127.0.0.1:2181",
                5000,
                500,
                retryPolicy);
        session.usingNamespace("base");

        // 启动会话
        session.start();

        return session;

    }

    /**
     * 用Builder模式创建会话并启动
     * @return CuratorFramework
     */
    private static CuratorFramework createSessionWithFluent() {
        CuratorFramework session = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:2181")
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(500)
                .retryPolicy(new RetryOneTime(200))
                // 为每个业务分配指定的命名空间，即根路径
                .namespace("base")

                .build();
        // 启动会话
        session.start();

        return session;
    }

    /**
     * 创建节点
     * 注意：Zookeeper规定，除叶子节点外，其他节点必须是持久节点
     * @param session
     */
    private static void crate(CuratorFramework session) {
        try {
            /**
             * 创建节点，默认是持久化的
             * path：路径
             * data：数据
             */
            session.create()
                    // 创建节点
                    .forPath("/", "init".getBytes());

            session.create()
                    // 设置为临时节点
                    .withMode(CreateMode.EPHEMERAL)
                    // 创建节点
                    .forPath("/", "init1".getBytes());

            session.create()
                    // 如果父节点不存在则递归创建
                    .creatingParentsIfNeeded()
                    // 设置为临时节点
                    .withMode(CreateMode.EPHEMERAL)
                    // 创建节点
                    .forPath("/", "init2".getBytes());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除节点
     * @param session
     */
    private static void delete(CuratorFramework session) {
        try {
            // 只能删除叶子节点
            session.delete()
                    .forPath("/init");

            // 删除节点及叶子节点
            session.delete()
                    .deletingChildrenIfNeeded()
                    .forPath("init3");

            // 强制删除成功，如果不成功，会一直执行删除操作直到删除成功
            session.delete()
                    // 担保，确保删除成功！如遇到网络异常删除失败时会重试，直到删除成功
                    .guaranteed()
                    .deletingChildrenIfNeeded()
                    .forPath("/init2");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 自定义重试策略
     */
    static class CustomRetryPolicy implements RetryPolicy {
        /**
         *
         * @param retryCount 已经重试的次数
         * @param elapsedTimeMs 重试花费的时间
         * @param sleeper
         * @return
         */
        @Override
        public boolean allowRetry(int retryCount, long elapsedTimeMs, RetrySleeper sleeper) {
            return false;
        }
    }
}
