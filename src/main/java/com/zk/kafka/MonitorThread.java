package com.zk.kafka;

/**
 * Created by ZK on 2017/5/4.
 */
public class MonitorThread implements Runnable{
    private  String address ;
    private String lowTopicName;
    private String highTopicName;
    private String brokerName;
    private int thresholdNum;
    private int durNum;
    private String serviceName;
    public MonitorThread(String serviceName,String zkAddress,String lowTopic,String highTopic,String broker,int threshold,int dur) {
        this.address = zkAddress;
        this.brokerName = broker;
        this.highTopicName = highTopic;
        this.lowTopicName = lowTopic;
        this.thresholdNum = threshold;
        this.durNum = dur;
        this.serviceName = serviceName;
        ZooKeeperServiceRegistry.register(serviceName,"0",zkAddress);
    }

    @Override
    public void run() {
        while (true) {
            long numHigh = KafkaMonitor.getKafkaNum(highTopicName, brokerName, address);
            long numLow = KafkaMonitor.getKafkaNum(lowTopicName, brokerName, address);
            if (numHigh+numLow > thresholdNum){
                ZooKeeperServiceRegistry.update(serviceName,"1",address);
            }else {
                ZooKeeperServiceRegistry.update(serviceName,"0",address);
            }

            System.out.println("当前状态：  "+KafkaMonitor.getQueueStatus(address,serviceName));
            try {
                Thread.sleep(durNum);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}
