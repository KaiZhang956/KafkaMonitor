package com.zk.kafka;

/**
 * Created by kf0454 on 2017/3/7.
 */

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class KafkaMonitor {
    private static final Logger log = LoggerFactory.getLogger(KafkaMonitor.class);
    public KafkaMonitor() {
//      m_replicaBrokers = new ArrayList<String>();
    }


    public static long getKafkaNum(String topic,String kafkaBroker,String zookeeper){
        int port = 9092;
        List<String> seeds = new ArrayList<String>();
        seeds.add(kafkaBroker);
        KafkaMonitor kot = new KafkaMonitor();

        TreeMap<Integer, PartitionMetadata> metadatas = kot.findLeader(seeds, port, topic);

        long sum = 0;
        long offsets = 0;
        for (Map.Entry<Integer, PartitionMetadata> entry : metadatas.entrySet()) {
            int partition = entry.getKey();
            String leadBroker = entry.getValue().leader().host();
            String clientName = "Client_" + topic + "_" + partition;
            SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, 100000,
                    64 * 1024, clientName);
            long readOffset = KafkaMonitor.getLastOffset(consumer, topic, partition,
                    kafka.api.OffsetRequest.LatestTime(), clientName);
            sum += readOffset;
            try {
                offsets += KafkaMonitor.getOffsets(topic, partition,zookeeper+":2181");
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //    System.out.println(partition+":"+readOffset);
            if (consumer != null) consumer.close();
        }

        long remain = sum - offsets;
        System.out.println("总和：" + sum);
        System.out.println("offsets：" + offsets);
        System.out.println("remain：" + remain);
        if (remain < 0) {
            remain = 0;
        }
        return remain;

    }

    public static String getQueueStatus(String zookeeper,String serviceName){
        return ZooKeeperServiceDiscovery.discover(zookeeper,serviceName);
    }

    private static long getLastOffset(SimpleConsumer consumer, String topic,
                                     int partition, long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
                partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
                whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
                clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
//            System.out
//                    .println("Error fetching data Offset Data the Broker. Reason: "
//                            + response.errorCode(topic, partition));
   log.error("Error fetching data Offset Data the Broker. Reason: "
                            + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
//      long[] offsets2 = response.offsets(topic, 3);
        return offsets[0];
    }

    private static TreeMap<Integer,PartitionMetadata> findLeader(List<String> a_seedBrokers,
                                                          int a_port, String a_topic) {
        TreeMap<Integer, PartitionMetadata> map = new TreeMap<Integer, PartitionMetadata>();
        loop: for (String seed : a_seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024,
                        "leaderLookup"+new Date().getTime());
                List<String> topics = Collections.singletonList(a_topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        map.put(part.partitionId(), part);
                    }
                }
            } catch (Exception e) {
//                System.out.println("Error communicating with Broker [" + seed
//                        + "] to find Leader for [" + a_topic + ", ] Reason: " + e);
                log.error("Error communicating with Broker [" + seed
                        + "] to find Leader for [" + a_topic + ", ] Reason: " + e);
            } finally {
                if (consumer != null)
                    consumer.close();
            }
        }
        return map;
    }
    private static Long getOffsets(String topic, int partition,String zookeeper) throws IOException, InterruptedException {

        final CountDownLatch connectedSemaphore = new CountDownLatch(1);
        ZooKeeper zooKeeper = new ZooKeeper(zookeeper, 20000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                Event.KeeperState keeperState = watchedEvent.getState();

                Event.EventType eventType = watchedEvent.getType();

                if (Event.KeeperState.SyncConnected == keeperState){
                    if (Event.EventType.None == eventType){
                        connectedSemaphore.countDown();
                    }
                }

            }
        });

          connectedSemaphore.await();

        byte [] data =  "0".getBytes();
        String path = "/consumers/"+topic+"/offsets/"+topic+"/" + partition;
        try {
            data = zooKeeper.getData(path, true, null);
        } catch (KeeperException e) {
            data = "0".getBytes();
        }
        if (zooKeeper!=null) zooKeeper.close();
        return Long.parseLong(new String(data));


    }
}
