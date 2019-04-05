package com.emotibot.test;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class MySpout implements IRichSpout {

    private static final long serialVersionUID = 1L;

    FileInputStream fis;
    InputStreamReader isr;
    BufferedReader br;

    SpoutOutputCollector collector = null;
    String str = null;


    public void nextTuple() {
        try {
            while ((str = this.br.readLine()) != null) {
                // 过滤动作
                collector.emit(new Values(str, str.split("\t")[1]));
            }
        } catch (Exception e) {
        }

    }


    public void close() {
        try {
            br.close();
            isr.close();
            fis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void activate() {

    }

    public void deactivate() {

    }


    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try {
            this.collector = collector;
            this.fis = new FileInputStream("input/track.log");
            this.isr = new InputStreamReader(fis, "UTF-8");
            this.br = new BufferedReader(isr);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("log", "session_id"));
    }


    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


    public void ack(Object msgId) {
        System.out.println("spout ack:" + msgId.toString());
    }


    public void fail(Object msgId) {
        System.out.println("spout fail:" + msgId.toString());
    }

}