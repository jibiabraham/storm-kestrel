package pg.data.storm.rollingcounter

import java.io.Serializable
import collection.mutable.{HashMap => MutableHashMap, HashSet}

class SlotBasedCounter[T](slots: Int) extends Serializable {
	private val objToCounts: MutableHashMap[T, List[Long]] = new MutableHashMap[T, List[Long]]
	private var numSlots: Int = _

	def apply(numSlots: Int) = {
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

	def getCounts(): MutableHashMap[T, Long] = {
		val result: MutableHashMap[T, Long] = new MutableHashMap[T, Long]()
		for (k: T <- objToCounts.keySet){
			result.put(k, computeTotalCount(k))
		}
		result
	}

	private def computeTotalCount(obj: T): Long = {
		val curr: List[Long] = objToCounts.get(obj) match {
			case None => List.fill(this.numSlots)(0)
			case Some(counts) => counts
		}
		var sum: Long = 0
		curr.foreach(sum += _)
		sum
	}

	def wipeSlot(slot: Int) = {
		for( k: T <- objToCounts.keySet) {
			resetSlotCountToZero(k, slot)
		}
	}

	private def resetSlotCountToZero(k: T, slot: Int) = {
		objToCounts.get(k) match {
			case None => 
			case Some(counts) => counts.updated(slot, 0)
		}
	}

	private def shouldBeRemovedFromCounter(k: T): Boolean = {
		computeTotalCount(k) == 0
	}

	def wipeZeroes = {
		val objToBeRemoved: HashSet[T] = new HashSet[T]()
		for(k: T <- objToCounts.keySet) {
			if(shouldBeRemovedFromCounter(k)){
				objToBeRemoved.add(k)
			}
		}
		for(k: T <- objToBeRemoved) {
			objToCounts.remove(k)
		}
	}
}