package com.emotibot.test;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Map;

public class MyBolt implements IRichBolt {

    private static final long serialVersionUID = 1L;

    OutputCollector collector = null;
    int num = 0;
    String valueString = null;

    public void cleanup() {

    }

    public void execute(Tuple input) {
        try {
            valueString = input.getStringByField("log");

            if (valueString != null) {
                num++;
                System.err.println(input.getSourceStreamId() + " " + Thread.currentThread().getName() + "--id="
                        + Thread.currentThread().getId() + "   lines  :" + num + "   session_id:"
                        + valueString.split("\t")[1]);
            }
            collector.ack(input);
            // Thread.sleep(2000);
        } catch (Exception e) {
            collector.fail(input);
            e.printStackTrace();
        }

    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(""));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
