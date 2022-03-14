name := "spark-property-tests"
organization := "com.zeotap"

publishMavenStyle := true
val sparkVersion = settingKey[String]("Spark Version")
val sparkTestingBaseVersion = settingKey[String]("Spark Testing Base Version")

sparkVersion := System.getProperty("sparkVersion", "2.4.3")
sparkTestingBaseVersion := System.getProperty("sparkTestingBaseVersion", "2.4.3_0.14.0")

scalaVersion := "2.11.12"
version := sparkVersion.value

libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % sparkTestingBaseVersion.value

// publish configurations
licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
homepage := Some(url("https://github.com/zeotap/spark-property-tests"))
scmInfo := Some(ScmInfo(
  url("https://github.com/zeotap/spark-property-tests.git"),
  "scm:git@github.com:zeotap/spark-property-tests.git"
))

pomIncludeRepository := { _ => false }
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

credentials += Credentials(Path.userHome / ".sbt" / "scredentials")

