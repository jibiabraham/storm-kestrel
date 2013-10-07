package pg.data.storm.rollingcounter

import backtype.storm.Constants
import backtype.storm.tuple.Tuple

object TupleHelpers {
	
	def isTickTuple(tuple: Tuple): Boolean = {
		tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}
}