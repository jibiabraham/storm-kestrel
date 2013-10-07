package pg.data.storm.rollingcounter

import java.io.Serializable
import collection.mutable.ListBuffer


class Rankings(topN: Int) extends Serializable {
	
	private var maxSize: Int = _
	private var rankedItems: List[Rankable] = List[Rankable]()

	def apply(topN: Int) = {
		if (topN < 1){
			throw new IllegalArgumentException("topN must be >= 1")
		}
		maxSize = topN
	}

	def getMaxSize: Int = {
		maxSize
	}

	def size: Int = {
		rankedItems.size
	}

	def getRankings: List[Rankable] = {
		rankedItems
	}

	def updateWith(other: Rankings): Unit = {
		for(r: Rankable <- other.getRankings) {
			updateWith(r)
		}
	}

	def updateWith(r: Rankable): Unit = {
		addOrReplace(r)
		rerank
		shrinkRankingsIfNeeded
	}

	private def addOrReplace(r: Rankable) = {
		val buffer = ListBuffer.empty ++= rankedItems
		val rank = findRankOf(r) match {
			case None => buffer.append(r)
			case Some(ranking) => buffer.update(ranking, r)
		}

		rankedItems = buffer.toList
	}

	private def findRankOf(r: Rankable): Option[Int] = {
		val tag = r.getObject
		return Option(rankedItems.indexWhere{ item => item.getObject.equals(tag) })
	}

	private def rerank = {
		rankedItems.sorted.reverse
	}

	private def shrinkRankingsIfNeeded = {
		if(rankedItems.size > maxSize){
			rankedItems = rankedItems.take(maxSize)
		}
	}

	def pruneZeroCounts = {
		var buffer = ListBuffer.empty ++= rankedItems.filter(item => item.getCount != 0)
		rankedItems = buffer.toList
	}
}