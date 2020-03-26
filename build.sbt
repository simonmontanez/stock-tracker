resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

name := "spark-stock-tracking"

version := "0.0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided"

libraryDependencies += "mrpowers" % "spark-daria" % "0.35.2-s_2.11"

libraryDependencies += "MrPowers" % "spark-fast-tests" % "0.20.0-s_2.11" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.1.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.1.2"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1"

// test suite settings
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
// Show runtime of tests
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

// JAR file settings

// don't include Scala in the JAR file
//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// Add the JAR file naming conventions described here: https://github.com/MrPowers/spark-style-guide#jar-files
// You can add the JAR file naming conventions by running the shell script
