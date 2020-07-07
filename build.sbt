import Dependencies._

ThisBuild / scalaVersion     := "2.13.2"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "io.timely0"
ThisBuild / organizationName := "timely0"

lazy val root = (project in file("."))
  .settings(
    name := "timely0",
    libraryDependencies += "com.lihaoyi" %% "castor" % "0.1.1"
  )
