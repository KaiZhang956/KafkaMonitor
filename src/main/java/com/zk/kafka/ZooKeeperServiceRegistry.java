package com.zk.kafka;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ZK on 2017/5/4.
 */
public class ZooKeeperServiceRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperServiceRegistry.class);

    public static void register(String serviceName, String status ,String zkAddress) {
        ZkClient zkClient= null;
        try {
            // 创建 ZooKeeper 客户端
            zkClient = new ZkClient(zkAddress, Constant.ZK_SESSION_TIMEOUT, Constant.ZK_CONNECTION_TIMEOUT);
            LOGGER.debug("connect zookeeper");
            // 创建 registry 节点（持久）
            String registryPath = Constant.ZK_REGISTRY_PATH;
            if (!zkClient.exists(registryPath)) {
                zkClient.createPersistent(registryPath);
                LOGGER.debug("create registry node: {}", registryPath);
            }
            // 创建 service 节点（持久）
            String servicePath = registryPath + "/" + serviceName;
            if (!zkClient.exists(servicePath)) {
                zkClient.createPersistent(servicePath,status);
                LOGGER.debug("create service node: {}", servicePath);
            }
//        // 创建 address 节点（临时）
//        String addressPath = servicePath + "/address-";
//        String addressNode = zkClient.createEphemeralSequential(addressPath, serviceAddress);
//        LOGGER.debug("create address node: {}", addressNode);
        } catch (Exception ex){
            ex.printStackTrace();
        }finally {
            zkClient.close();
        }
    }
    public static void update(String serviceName, String status ,String zkAddress) {
        ZkClient zkClient= null;
        try {
            // 创建 ZooKeeper 客户端
            zkClient = new ZkClient(zkAddress, Constant.ZK_SESSION_TIMEOUT, Constant.ZK_CONNECTION_TIMEOUT);
            LOGGER.debug("connect zookeeper");
            // 创建 registry 节点（持久）
            String registryPath = Constant.ZK_REGISTRY_PATH;
            if (!zkClient.exists(registryPath)) {
                zkClient.createPersistent(registryPath);
                LOGGER.debug("create registry node: {}", registryPath);
            }
            // 创建 service 节点（持久）
            String servicePath = registryPath + "/" + serviceName;
            if (!zkClient.exists(servicePath)) {
                zkClient.createPersistent(servicePath,status);
                LOGGER.debug("create service node: {}", servicePath);
            }
            zkClient.writeData(servicePath,status);
//        // 创建 address 节点（临时）
//        String addressPath = servicePath + "/address-";
//        String addressNode = zkClient.createEphemeralSequential(addressPath, serviceAddress);
//        LOGGER.debug("create address node: {}", addressNode);
        } catch (Exception ex){
            ex.printStackTrace();
        }finally {
                 zkClient.close();
        }
    }
}