package com.pg.storm.kestrel.testtopology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.scheme.StringScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.util.Map;


object TestTopology {
    class FailEveryOther extends BaseRichBolt {
        
        var _collector:OutputCollector = _
        var i = 0
        
        @Override
        def prepare(map: Map, tc: TopologyContext, collector: OutputCollector) {
            _collector = collector
        }

        @Override
        def execute(tuple: Tuple) {
            i += 1 
            if(i%2==0) {
                _collector.fail(tuple)
            } else {
                _collector.ack(tuple)
            }
        }
        
        @Override
        def declareOutputFields(declarer: OutputFieldsDeclarer) {
        }
    }
    
    def main(args: Array[String]) {
        builder = new TopologyBuilder()
        spout = new KestrelThriftSpout("localhost", 2229, "test", new StringScheme())
        builder.setSpout("spout", spout).setDebug(true)
        builder.setBolt("bolt", new FailEveryOther())
                .shuffleGrouping("spout")
        
        cluster = new LocalCluster()
        conf = new Config()
        cluster.submitTopology("test", conf, builder.createTopology())
        
        Thread.sleep(600000)
    }
}