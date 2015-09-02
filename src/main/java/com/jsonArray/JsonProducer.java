package com.jsonArray;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import java.util.Properties;
import org.apache.log4j.Logger;

/**
 *
 * @author Munim Ali
 */
public class JsonProducer {

    private static final Logger LOG = Logger.getLogger(SimpleProducer.class);

    public static void main(String[] args) 
    {
        if (args.length != 2) 
        {
            
            System.out.println("Usage: SimpleProducer <broker list> <zookeeper>");
            System.exit(-1);
        }
        
        LOG.debug("Using broker list:" + args[0] +", zk conn:" + args[1]);

        
        Properties props = new Properties();
        props.put("metadata.broker.list", args[0]);
        props.put("zk.connect", args[1]);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        String TOPIC = "jsonEvents";
        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);

        String[] json_events = {
            "{ \"request\" : { \"user\" : \"Mario\", \"location\" : \"His house.\", \"siblings\" : [\"Luigi\"], \"countries\" : [ { \"name\" : \"USA\", \"regions\": [\"California\", \"New York\", \"Washington\"] }, { \"name\" : \"Japan\", \"regions\" : [\"Tokyo\", \"Osaka\", \"Aichi\"] }, { \"name\" : \"Italy\", \"regions\" : [\"Lazio\", \"Lombardy\", \"Veneto\"] } ], \"id\" : 619 }, \"flags\" : { \"activated\" : false }, \"meta\" : { \"name\" : \"Alpha\" } }"
            , "{ \"request\" : { \"user\" : \"Luigi\", \"location\" : \"His house.\", \"siblings\" : [\"Luigi\"], \"countries\" : [ { \"name\" : \"India\", \"regions\": [\"Karnataka\", \"Maharashtra\", \"West Bengal\"] }, { \"name\" : \"UK\", \"regions\" : [\"England\", \"Wales\", \"Scotland\"] }, { \"name\" : \"UAE\", \"regions\" : [\"Dubai\", \"Abu Dhabi\", \"Sharjah\"] } ], \"id\" : 620 }, \"flags\" : { \"activated\" : false }, \"meta\" : { \"name\" : \"Alpha\" } }"

        };

        for (String x : json_events) {
            try {
                KeyedMessage<String, String> message =new KeyedMessage<String, String>(TOPIC, x);
                producer.send(message);
                Thread.sleep(1000);
            } catch (Exception e) {
                    e.printStackTrace();
            }
        }
        
        producer.close();
    }
    
}
