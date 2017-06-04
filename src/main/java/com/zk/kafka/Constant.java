package com.zk.kafka;

/**
 * Created by ZK on 2017/5/4.
 */
public interface Constant {

    int ZK_SESSION_TIMEOUT = 5000;
    int ZK_CONNECTION_TIMEOUT = 1000;

    String ZK_REGISTRY_PATH = "/kafkamonitor";
    String ZK_HIGH_PATH = "highrequest";
    String ZK_BACKUP_PATH = "highrequestbackup";
}