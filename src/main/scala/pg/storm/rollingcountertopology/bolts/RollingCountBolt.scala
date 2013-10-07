package pg.data.storm.rollingcounter

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;

class RollingCountBolt(private var windowLengthInSeconds: Int, private var emitFrequencyInSeconds: Int) extends BaseRichBolt {
	val LOG: Logger = Logger.getLogger(this.getClass)	
	val NUM_WINDOW_CHUNKS = 5
	val DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 60
	val DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS
	val WINDOW_LENGTH_WARNING_TEMPLATE = "Actual window length is %d seconds when it should be %d seconds (you can safely ignore this warning during the startup phase)";

	private var counter: SlidingWindowCounter[Any] = _
	private var collector: OutputCollector = _
	private var lastModifiedTracker: NthLastModifiedTimeTracker = _

	def apply(): RollingCountBolt = {
		this.apply(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS)
	}

	def apply(windowLengthInSeconds: Int, emitFrequencyInSeconds: Int): RollingCountBolt = {	
		counter = new SlidingWindowCounter[Any](
			deriveNumWindowChunksFrom(windowLengthInSeconds, emitFrequencyInSeconds)
		)
		this
	}

	private def deriveNumWindowChunksFrom(windowLengthInSeconds: Int, windowUpdateFrequencyInSeconds: Int) = {
		windowLengthInSeconds / windowUpdateFrequencyInSeconds
	}

	override def prepare(stormConf: Any, context: TopologyContext, collector: OutputCollector) {
		this.collector = collector;
		lastModifiedTracker = new NthLastModifiedTimeTracker(deriveNumWindowChunksFrom(this.windowLengthInSeconds, this.emitFrequencyInSeconds));
	}

	override def execute(tuple: Tuple) = {
		if (TupleHelpers.isTickTuple(tuple)) {
			LOG.debug("Received tick tuple, triggering emit of current window counts");
			emitCurrentWindowCounts;
		}
		else {
			countObjAndAck(tuple);
		}
	}

	private def emitCurrentWindowCounts = {
		val counts: collection.mutable.HashMap[Any, Long] = counter.getCountsThenAdvanceWindow
		val actualWindowLengthInSeconds: Int = lastModifiedTracker.secondsSinceOldestModification
		lastModifiedTracker.markAsModified
		if (actualWindowLengthInSeconds != windowLengthInSeconds){
			//LOG.warn(String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
		}
		emit(counts, actualWindowLengthInSeconds)
	}

	private def emit(counts: collection.mutable.HashMap[Any, Long], actualWindowLengthInSeconds: Int) = {
		for (k: AnyRef <- counts.keySet){
			collector.emit(new Values(k, counts.get(k) match {
				case None => 0
				case Some(count: Long) => count	
			}, actualWindowLengthInSeconds))
		}
	}

	private def countObjAndAck(tuple: Tuple) = {
		val obj = tuple.getValue(0)
		counter.incrementCount(obj)
		collector.ack(tuple)
	}

	override def declareOutputFields(declarer: OutputFieldsDeclarer) = {
		declarer.declare(new Fields("obj", "count", "actualWindowLengthInSeconds"));
	}
}