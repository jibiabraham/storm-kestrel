package pg.data.storm.rollingcounter

import backtype.storm.Config;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;

import collection.immutable.HashMap


abstract class AbstractRankerBolt(topN: Int, emitFreq: Int) extends BaseBasicBolt {
	private val DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2
	private val DEFAULT_COUNT = 10

	private var emitFrequencyInSeconds: Int = _
	private var count: Int = _
	private var rankings: Rankings = _

	def apply(): AbstractRankerBolt = {
		this.apply(DEFAULT_COUNT, DEFAULT_EMIT_FREQUENCY_IN_SECONDS)
	}

	def apply(topN: Int): AbstractRankerBolt = {
		this.apply(topN, DEFAULT_EMIT_FREQUENCY_IN_SECONDS)
	}

	def apply(topN: Int, emitFrequencyInSeconds: Int): AbstractRankerBolt = {
		if (topN < 1) {
			throw new IllegalArgumentException("topN must be >= 1 (you requested " + topN + ")");
		}
		if (emitFrequencyInSeconds < 1) {
			throw new IllegalArgumentException("The emit frequency must be >= 1 seconds (you requested " + emitFrequencyInSeconds + " seconds)");
		}
		count = topN;
		this.emitFrequencyInSeconds = emitFrequencyInSeconds;
		rankings = new Rankings(count);
		this
	}

	protected def getRankings: Rankings = {
		rankings
	}

	override def execute(tuple: Tuple, collector: BasicOutputCollector) = {
		if (TupleHelpers.isTickTuple(tuple)){
			getLogger.debug("Recieved tick tuple, triggering emit of current rankings")
			emitRankings(collector)
		}
		else {
			updateRankingsWithTuple(tuple)
		}
	}

	def updateRankingsWithTuple(tuple: Tuple)
	
	private def emitRankings(collector: BasicOutputCollector) = {
		collector.emit(new Values(rankings))
		getLogger.debug("Rankings " + rankings)
	}

	override def declareOutputFields(declarer: OutputFieldsDeclarer) = {
		declarer.declare(new Fields("rankings"))
	}

	def getLogger: Logger
}