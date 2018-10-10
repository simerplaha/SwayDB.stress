name := "SwayDB.stress"

version := "0.1"

scalaVersion := "2.12.7"

resolvers += Opts.resolver.mavenLocalFile
resolvers += Opts.resolver.sonatypeReleases

libraryDependencies ++=
  Seq(
    "com.github.simerplaha" %% "actor" % "0.2.1",
    "io.swaydb" %% "swaydb" % "0.5",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
    "io.suzaku" %% "boopickle" % "1.2.6",
    "org.scalatest" %% "scalatest" % "3.0.4" % Test,
    "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % Test,
    "ch.qos.logback" % "logback-classic" % "1.2.3" % Test
  )