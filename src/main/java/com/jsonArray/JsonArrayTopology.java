package com.jsonArray;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 *
 * @author Munim Ali
 */
public class JsonArrayTopology 
{

    private static final String READ_SPOUT_ID = "read-spout";
    private static final String PROCESS_BOLT_ID = "process-bolt";
    private static final String TOPOLOGY_NAME = "json-topology";
    
    public static void  main(String[] str) throws Exception
    {
    
        JsonSpout spout = new JsonSpout();
        ProcessBolt processBolt = new ProcessBolt();
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout(READ_SPOUT_ID, spout);
        
        builder.setBolt(PROCESS_BOLT_ID, processBolt)
                .shuffleGrouping(READ_SPOUT_ID);
 
        Config config = new Config();
        
        if (str.length == 0) //Local Cluster
        {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        
            Thread.sleep(10000);
        
            cluster.killTopology(TOPOLOGY_NAME);
            cluster.shutdown();
        }
        else //Remote Cluster
        {
            StormSubmitter.submitTopology(str[0], config, builder.createTopology());
        }
       
        
        
    }
}
