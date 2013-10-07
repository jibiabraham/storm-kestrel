package pg.data.storm.rollingcounter

import backtype.storm.tuple.Tuple
import java.io.Serializable
import collection.JavaConverters._


class RankableObjectWithFields(rankable: Any, rankableCount: Long, otherFields: Any*) extends Serializable with Rankable {
	private var obj: Any = _
	private var count: Long = _
	private var fields: List[Any] = _

	def apply(rankable: Any, rankableCount: Long, otherFields: Any*) = {
		if (rankable == null) {
			throw new IllegalArgumentException("The object must not be null");
		}
		if (rankableCount < 0) {
			throw new IllegalArgumentException("The count must be >= 0");
		}

		this.obj = rankable;
		this.count = rankableCount;
		this.fields = otherFields.toList
	}

	def from(tuple: Tuple): RankableObjectWithFields = {
		val otherFields: List[Any] = tuple.getValues.asScala.toList
		val rankable = otherFields.head
		val remaining = otherFields.tail
		val count: Long = remaining.head match {
			case None => 0
			case Some(number: Long) => number 
		}
		val trackableFields = remaining.tail
		new RankableObjectWithFields(rankable, count, trackableFields)
	}

	def getObject: Any = {
		obj
	}

	def getCount: Long = {
		count
	}

	def getFields: List[Any] = {
		fields
	}

	override def compareTo(other: Rankable) = {
		val delta: Long = this.getCount - other.getCount
		if (delta > 0){
			1
		}
		else if (delta < 0){
			-1
		}
		else {
			0
		}
	}

	override def equals(o: Any): Boolean = {
		if(this == o){
			true
		}
		o match {
			case other: RankableObjectWithFields => 
				obj.equals(other.obj) && count == other.count
			case _ => false
		}
	}

	override def hashCode: Int = {
		var result = 17
		val countHash: Int = (count ^ (count >>> 32)).toInt
		result = 31 * result + countHash
		result = 31 + result + obj.hashCode
		result
	}

	override def toString: String = {
		val buf = new StringBuffer
		buf.append("[")
		buf.append(obj)
		buf.append("|")
		buf.append(count)
		for( field: Any <- fields) {
			buf.append("|")
			buf.append(field)
		}
		buf.append("]")
		buf.toString
	}
}