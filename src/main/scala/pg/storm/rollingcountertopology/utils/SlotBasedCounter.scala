package pg.data.storm.rollingcounter

import java.io.Serializable
import collection.mutable.{HashMap => MutableHashMap}
import collection.immutable.HashMap

class SlotBasedCounter[T]() extends Serializable {
	private val objToCounts: MutableHashMap[T, List[Long]] = new MutableHashMap[T, List[Long]]
	private var numSlots: Int = _

	def SlotBasedCounter(numSlots: Int) = {
		if (numSlots < 0){
			throw new IllegalArgumentException("Number of slots must be greater than zero (you requested " + numSlots + ")")
		}
		this.numSlots = numSlots
	}

	def incrementCount(obj: T, slot: Int) = {
		objToCounts get obj match {
			case None => objToCounts(obj) = List.fill(this.numSlots)(0)
			case Some(counts) => objToCounts(obj) = counts.updated(slot, counts(slot) + 1)
		}
	}

	def getCount(obj: T, slot: Int): Long = {
		objToCounts get obj match {
			case None => 0
			case Some(counts) => counts(slot)
		}
	}

	def getCounts(): HashMap[T, Long] = {
		val result: HashMap[T, Long] = new HashMap[T, Long]()
		objToCounts.keySet() forEach { obj => 
			result.put(obj, computeTotalCount(obj))
		}
		result
	}

	private def computeTotalCount(obj: T) = {
		val curr: List[Long] = objToCounts.get(obj) match {
			case None => List.fill(this.numSlots)(0)
			case Some(counts) => counts
		}
		var total = 0
		curr forEach { num =>
			total += num 
		}
		total
	}
}