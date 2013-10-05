name := "kestrel_tests"

version := "0.0.1"

scalaVersion := "2.9.2"

resolvers ++= Seq(
    "clojars" at "http://clojars.org/repo/"
)

libraryDependencies ++=Seq(
	"com.twitter" %% "finagle-core" % "6.6.2",
    "com.twitter" %% "finagle-kestrel" % "6.6.2",
    "com.twitter" %% "finagle-thrift" % "6.6.2",
	"com.twitter.common.zookeeper" % "server-set" % "1.0.42",
	"storm" % "storm" % "0.9.0-wip15",
	"storm" % "storm-kestrel" % "0.9.0-wip5-multischeme",
	"com.github.velvia" % "scala-storm_2.9.1" % "0.2.2"
)

com.twitter.scrooge.ScroogeSBT.newSettings

com.twitter.scrooge.ScroogeSBT.scroogeBuildOptions := Seq("--finagle")

libraryDependencies ++= Seq(
  "org.apache.thrift" % "libthrift" % "0.8.0",
  "com.twitter" %% "scrooge-runtime" % "3.1.5"
)