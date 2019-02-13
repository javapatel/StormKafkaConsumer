package com.storm;

import org.apache.log4j.Logger;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.HashMap;

/**
 * @author Amit Kumar
 */
public class StormLauncher {

    private static final Logger LOG = Logger.getLogger(StormLauncher.class);

    public static void main(String[] args) throws InterruptedException {
        // Build Spout configuration using input command line parameters
        final BrokerHosts zkrHosts = new ZkHosts("localhost:2181");
        final String kafkaTopic = "sample.request";


        final String zkRoot = "D:\\tmp\\kafka-logs\\";
        final String clientId = "storm2";
        final SpoutConfig kafkaConf = new SpoutConfig(zkrHosts, kafkaTopic, zkRoot, clientId);
        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        // Build topology to consume message from kafka and print them on console
        final TopologyBuilder topologyBuilder = new TopologyBuilder();
        // Create KafkaSpout instance using Kafka configuration and add it to topology
      //  kafkaConf.startOffsetTime=1L;
       // kafkaConf.useStartOffsetTimeIfOffsetOutOfRange=true;
        KafkaSpout spout = new KafkaSpout(kafkaConf);

        topologyBuilder.setSpout("kafka-spout", spout, 1);
        //Route the output of Kafka Spout to Logger bolt to log messages consumed from Kafka
        topologyBuilder.setBolt("print-messages", new ConsumerBolt()).globalGrouping("kafka-spout");

        // Submit topology to local cluster i.e. embedded storm instance in eclipse
        final LocalCluster localCluster = new LocalCluster();
        StormTopology topology = topologyBuilder.createTopology();
        localCluster.submitTopology("kafka-topology", new HashMap(), topology);
//        Thread.sleep(10000000);
//        localCluster.killTopology("kafka-topology");
//        localCluster.shutdown();
    }
    // SAP Notifier, Account Specific Notifier, Email Notifier
}
