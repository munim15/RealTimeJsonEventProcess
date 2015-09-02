package com.jsonArray;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.StringScheme;

import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;


/**
 *
 * @author Munim Ali
 */
public class SimpleTopology 
{   
    private static final Logger LOG = Logger.getLogger(SimpleTopology.class);

    public static class PrinterBolt extends BaseBasicBolt {
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            System.out.println("LOOK HERE %%%%%%%%%%%%%%%%%%%% : " + tuple.toString());
        }

    }

    private static final String KAFKA_SPOUT_ID = "kafkaSpout"; 
    private static final String LOG_PRINTER_BOLT_ID = "PrinterBolt";

    protected Properties topologyConfig;

    public SimpleTopology(String configFileLocation) throws Exception {
        topologyConfig = new Properties();
        try {
            topologyConfig.load(ClassLoader.getSystemResourceAsStream(configFileLocation));
        } catch (FileNotFoundException e) {
            LOG.error("Encountered error while reading configuration properties: "
                    + e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error("Encountered error while reading configuration properties: "
                    + e.getMessage());
            throw e;
        }           
    }

    private SpoutConfig constructKafkaSpoutConf() 
    {
        BrokerHosts hosts = new ZkHosts(topologyConfig.getProperty("kafka.zookeeper.host.port"));
        String topic = topologyConfig.getProperty("kafka.topic");
        String zkRoot = topologyConfig.getProperty("kafka.zkRoot");
        String consumerGroupId = "StormSpout";

        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, consumerGroupId);

        /* Custom TruckScheme that will take Kafka message of single truckEvent 
         * and emit a 2-tuple consisting of truckId and truckEvent. This driverId
         * is required to do a fieldsSorting so that all driver events are sent to the set of bolts */
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        return spoutConfig;
    }
    
    
    
    public void configureKafkaSpout(TopologyBuilder builder) 
    {
        KafkaSpout kafkaSpout = new KafkaSpout(constructKafkaSpoutConf());
        int spoutCount = Integer.valueOf(topologyConfig.getProperty("spout.thread.count"));
        builder.setSpout(KAFKA_SPOUT_ID, kafkaSpout);
    }
    
    public void configurePrinterBolt(TopologyBuilder builder)
    {
        PrinterBolt bolt = new PrinterBolt();
        builder.setBolt(LOG_PRINTER_BOLT_ID, bolt).globalGrouping(KAFKA_SPOUT_ID);
    }
    
    private void buildAndSubmit() throws Exception
    {
        TopologyBuilder builder = new TopologyBuilder();
        configureKafkaSpout(builder);
        configurePrinterBolt(builder);
        
        Config conf = new Config();
        conf.setDebug(true);
        
        StormSubmitter.submitTopology("simple-string-processor", 
                                    conf, builder.createTopology());
    }

    public static void main(String[] str) throws Exception
    {
        String configFileLocation = "simple_string_topology.properties";
        SimpleTopology topo
                = new SimpleTopology(configFileLocation);
        topo.buildAndSubmit();
    }
}
