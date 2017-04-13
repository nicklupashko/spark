name := "spark"

version := "1.0"

scalaVersion := "2.11.0"

resolvers ++= Seq(
  "Twitter"                       at "http://maven.twttr.com",
  "Maven Central Server"          at "http://repo1.maven.org/maven2",
  "TypeSafe Repository Releases"  at "http://repo.typesafe.com/typesafe/releases/",
  "TypeSafe Repository Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/",
  "Sonatype"                      at "https://oss.sonatype.org/content/groups/public"
)

resolvers += Resolver.mavenLocal

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.scala-lang"   %  "scala-library"   % "2.11.0",
  "org.scala-lang"   %  "scala-xml"       % "2.11.0-M4",
  "org.apache.spark" %% "spark-core"      % sparkVersion,
  "org.apache.spark" %% "spark-sql"       % sparkVersion,
  "org.apache.spark" %% "spark-graphx"    % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion
)
