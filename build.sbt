name := "carrega_banco"

version := "2.2"

scalaVersion := "2.12.10"

scalacOptions := Seq("-deprecation", "-encoding", "utf8")

val sparkVersion = "3.1.1.3.1.7270.0-253"

unmanagedBase := baseDirectory.value /"libs"

unmanagedJars in Compile += file("*.jar")

resolvers += "cloudera repo" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies ++= Seq(

  "org.scala-lang" % "scala-library" % "2.12.12",

  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "org.apache.spark" %% "spark-streaming" % sparkVersion ,

)

dependencyOverrides ++= Seq(
  "org.apache.curator" % "curator-recipes" % "4.3.0.7.2.7.0-184"
)


mainClass in assembly := Some("Main")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}



