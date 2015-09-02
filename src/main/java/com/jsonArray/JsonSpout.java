package com.jsonArray;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

/**
 *
 * @author Munim Ali
 */
public class JsonSpout  extends BaseRichSpout{

    private String[] sentences = {
        "{ \"request\" : { \"user\" : \"Mario\", \"location\" : \"His house.\", \"siblings\" : [\"Luigi\"], \"countries\" : [ { \"name\" : \"USA\", \"regions\": [\"California\", \"New York\", \"Washington\"] }, { \"name\" : \"Japan\", \"regions\" : [\"Tokyo\", \"Osaka\", \"Aichi\"] }, { \"name\" : \"Italy\", \"regions\" : [\"Lazio\", \"Lombardy\", \"Veneto\"] } ], \"id\" : 619 }, \"flags\" : { \"activated\" : false }, \"meta\" : { \"name\" : \"Alpha\" } }"
    };
    private SpoutOutputCollector collector;
    private int index = 0;
    
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) 
    {
        declarer.declare(new Fields("json"));
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) 
    {
        this.collector = collector;
    }

    public void nextTuple() 
    {
        
            this.collector.emit(new Values(sentences[index]));
            index ++;
            if (index >= sentences.length) index = 0;
            Utils.sleep(1000);
        
    }
    
}
