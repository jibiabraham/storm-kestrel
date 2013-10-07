package pg.data.storm.rollingcounter

import backtype.storm.tuple.Tuple
import org.apache.log4j.Logger


class IntermediateRankingsBolt(topN: Int, emitFreq: Int) extends AbstractRankerBolt(topN, emitFreq) {
	val LOG: Logger = Logger.getLogger(this.getClass)
	
	override def updateRankingsWithTuple(tuple: Tuple) = {
		val rankable: Rankable = RankableObjectWithFields.from(tuple)
		this.getRankings.updateWith(rankable)
	}

	override def getLogger: Logger = {
		LOG
	}
}

