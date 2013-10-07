package pg.data.storm.rollingcounter

trait Rankable extends Comparable[Rankable] {
	def getObject: Any
	def getCount: Long
}