name := "SwayDB.stress"

version := "0.1"

scalaVersion := "2.12.4"

resolvers += Opts.resolver.mavenLocalFile
resolvers += Opts.resolver.sonatypeReleases

libraryDependencies ++=
  Seq(
    "io.swaydb" %% "swaydb" % "0.2.1",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
    "com.typesafe.akka" %% "akka-typed" % "2.5.4",
    "io.suzaku" %% "boopickle" % "1.2.6",
    "org.scalatest" %% "scalatest" % "3.0.4" % Test,
    "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % Test,
    "ch.qos.logback" % "logback-classic" % "1.2.3" % Test
  )