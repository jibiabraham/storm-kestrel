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

	private counter: SlidingWindowCounter[Any] = _
	private collector: OutputCollector = _
	private lastModifiedTracker: NthLastModifiedTimeTracker = _

	def apply: RollingCountBolt = {
		this.apply(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS)
	}

	def apply(windowLengthInSeconds, emitFrequencyInSeconds) = {	
		counter = new SlidingWindowCounter[Any](deriveNumWindowChunksFrom(windowLengthInSeconds), emitFrequencyInSeconds)
	}

	private def deriveNumWindowChunksFrom(windowLengthInSeconds: Int, windowUpdateFrequencyInSeconds: Int) = {
		windowLengthInSeconds / windowUpdateFrequencyInSeconds
	}
}