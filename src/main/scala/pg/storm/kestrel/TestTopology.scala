package com.pg.storm.kestrel.testtopology

import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.TopologyBuilder
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Tuple
import java.util.{Map => JMap}

import backtype.storm.spout.KestrelThriftSpout
import backtype.storm.scheme.StringScheme
import scala.collection.JavaConverters._

object TestTopology {

    class FailEveryOther extends BaseRichBolt {
        
        var _collector:OutputCollector = _
        var i = 0
        
        @Override
        def prepare(map: JMap[_, _], tc: TopologyContext, collector: OutputCollector) {
            _collector = collector
        }

        @Override
        def execute(tuple: Tuple) {
            Console.println("Got a new item from queue")
            _collector.ack(tuple)
        }
        
        @Override
        def declareOutputFields(declarer: OutputFieldsDeclarer) {
        }
    }
    
    def main(args: Array[String]) {
        var spout = new KestrelThriftSpout("localhost", 2229, "spam", new StringScheme)

        val builder = new TopologyBuilder
        var cluster = new LocalCluster
        
        builder.setSpout("spout", spout).setDebug(true)
        builder.setBolt("bolt", new FailEveryOther())
                .shuffleGrouping("spout")
        
        var conf = new Config()
        cluster.submitTopology("test", conf, builder.createTopology())
        
        Thread.sleep(600000)
    }
}