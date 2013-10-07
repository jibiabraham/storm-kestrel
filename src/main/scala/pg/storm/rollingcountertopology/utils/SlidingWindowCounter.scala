package pg.data.storm.rollingcounter

import java.io.Serializable
import collection.immutable.HashMap

class SlidingWindowCounter[T](windowLength: Int) extends Serializable {
	
	private var objCounter: SlotBasedCounter[T] = _
	private var headSlot: Int = _
	private var tailSlot: Int = _
	private var windowLengthInSlots: Int = _

	def apply(windowLengthInSlots: Int) = {
		if (windowLengthInSlots < 2){
			throw new IllegalArgumentException (
				"Window length in slots must be at least two (you requested " + windowLengthInSlots + ")"
			)
		}
		this.windowLengthInSlots = windowLengthInSlots
		this.objCounter = new SlotBasedCounter[T](this.windowLengthInSlots)

		this.headSlot = 0
		this.tailSlot = slotAfter(headSlot)
	}

	def incrementCount(obj: T) = {
		objCounter.incrementCount(obj, headSlot)
	}

	def getCountsThenAdvanceWindow(): collection.mutable.HashMap[T, Long] = {
		val counts: collection.mutable.HashMap[T, Long] = objCounter.getCounts()
		objCounter.wipeZeroes
		objCounter.wipeSlot(tailSlot)
		advanceHead
		counts
	}

	private def advanceHead = {
		headSlot = tailSlot
		tailSlot = slotAfter(headSlot)
	}

	private def slotAfter(slot: Int): Int = {
		(slot + 1) % windowLengthInSlots
	}
}