package com.jsonArray;
import java.lang.StringBuilder;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONArray;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.io.*;

/**
 *
 * @author Munim Ali
 */
public class ProcessBolt extends BaseRichBolt
{
	private PrintWriter pw;
	private File f;
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) 
    {
        //no field output.
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        f = new File("json_out.txt");
        try {
	        pw = new PrintWriter(f);
        } catch (Exception e) {
        }
    }

    public void execute(Tuple input) 
    {
    	try {
	    	String resource = "request.countries";
	        String json = input.getStringByField("json");
	        ArrayList<String> result = new ArrayList<String>();
	    	JSONObject obj = new JSONObject(json);
	    	String[] names = resource.toString().split("\\.");
	    	traverse(result, obj, names, 0);
	    	for (String x : result) {
	    		pw.println(x);
	    	}
    	} catch(JSONException j) {
    	}
    }

    private static void traverse(ArrayList<String> res, Object x, String[] names, int index) throws JSONException {
      if (index  == names.length) {
        if (x instanceof JSONArray) {
          JSONArray arr = (JSONArray) x;
          for (int i = 0; i < arr.length(); i += 1) {
              res.add((arr.get(i).toString()));
            }
        } else {
          res.add(((((JSONObject) x).toString())));
        }
          return;
      } else if (x instanceof JSONArray) {
          JSONArray arr = (JSONArray) x;
          boolean flag = false;
          try {
            arr.getJSONObject(0).getJSONObject(names[index]);
          } catch (Exception e) {
            flag = true;
          }
          for (int i = 0; i < arr.length(); i += 1) {
            if (flag) {
                traverse(res, arr.getJSONObject(i).getJSONArray(names[index]), names, index + 1);
              } else {
                traverse(res, arr.getJSONObject(i).getJSONObject(names[index]), names, index + 1);
              }
          } 
      } else {
        JSONObject temp = (JSONObject) x;
        boolean flag = false;
        try {
          temp.getJSONObject(names[index]);
        } catch (Exception e) {
          flag = true;
          try {
            temp.getJSONArray(names[index]);
          } catch (Exception exception) {
            //Path does not exist
            return;
          }
        }
        if (!flag) {
          traverse(res, temp.getJSONObject(names[index]), names, index + 1);
        } else {
          traverse(res, temp.getJSONArray(names[index]), names, index + 1);
        }
    }
  }
    
    public void cleanup() 
    {
    	pw.close();
        // System.out.println("------- Counts -------");
        // ArrayList<String> l = new ArrayList<String>(counts.keySet());
        // Collections.sort(l);
        // for (String key: l)
        // {
        //     System.out.println(key + "," + counts.get(key));
        // }
    }

    
}
