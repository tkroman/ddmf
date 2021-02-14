name := "modelfactory"

version := "0.1"

scalaVersion := "2.13.4"

libraryDependencies += "com.lihaoyi" %% "cask" % "0.7.3"
libraryDependencies += "com.lihaoyi" %% "utest" % "0.7.7" % "test"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"

testFrameworks += new TestFramework("utest.runner.Framework")

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-unchecked",
  "-language:postfixOps",
  "-language:higherKinds",
  "-Ymacro-annotations"
)

enablePlugins(DockerPlugin)

dockerfile in docker := {
  val artifact: File = assembly.value
  val artifactTargetPath = s"/app/${artifact.name}"

  new Dockerfile {
    from("openjdk:8-jre-alpine")
    add(artifact, artifactTargetPath)
    expose(8080)
    env("HISTORY_SIZE", "1024")
    env("NUMBER_OF_NODES", "4")
    entryPoint("java", "-jar", artifactTargetPath)
  }
}

imageNames in docker := Seq(
  ImageName(s"tkroman/ddmf:latest"),
  ImageName(
    namespace = Some("tkroman"),
    repository = "ddmf",
    tag = Some("v1")
  )
)

