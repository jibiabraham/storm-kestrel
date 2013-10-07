package pg.data.storm.rollingcounter

import backtype.storm.utils.Time
import org.apache.commons.collections.buffer.CircularFifoBuffer

class NthLastModifiedTimeTracker(numTimesToTrack: Int) {
	private val MILLIS_IN_SEC = 1000
	private var lastModifiedTimesMillis: CircularFifoBuffer = _

	def apply(numTimesToTrack: Int) = {
		if (numTimesToTrack < 1){
			throw new IllegalArgumentException("numTimesToTrack must be greater than zero (you requested " + numTimesToTrack + ")")
		}
		lastModifiedTimesMillis = new CircularFifoBuffer(numTimesToTrack)
		initLastModifiedTimesMillis
	}

	private def initLastModifiedTimesMillis = {
		val nowCached: Long = now
		for (i <- 0 until lastModifiedTimesMillis.maxSize) {
			lastModifiedTimesMillis.add(nowCached);
		}	
	}

	private def now: Long = {
		Time.currentTimeMillis
	}

	def secondsSinceOldestModification: Int  = {
		val modifiedTimeMillis: Long = lastModifiedTimesMillis.get.asInstanceOf[Long]
		(now - modifiedTimeMillis) / MILLIS_IN_SEC toInt
	}

	def markAsModified = {
		updateLastModifiedTime
	}

	def updateLastModifiedTime = {
		lastModifiedTimesMillis.add(now);
	}
}