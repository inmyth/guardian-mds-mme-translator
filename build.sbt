ThisBuild / version := "0.1.0-SNAPSHOT"

version := "0.1"

ThisBuild / scalaVersion := "2.13.10"

Compile / packageBin / mainClass := Some("me.mbcu.kafka.minimal.monix.Consumer")

lazy val root = (project in file("."))
  .settings(
    name := "guardian-mds-mme-translator",
    idePackagePrefix := Some("com.guardian")
  )

libraryDependencies += "org.apache.kafka"      %% "kafka-streams-scala" % "3.3.2"
dependencyOverrides += "org.apache.kafka"       % "kafka-clients"       % "3.3.2"
libraryDependencies += "io.monix"              %% "monix-kafka-1x"      % "1.0.0-RC6"
libraryDependencies += "io.monix"              %% "monix"               % "3.4.1"
libraryDependencies += "org.typelevel"         %% "cats-core"           % "2.9.0"
libraryDependencies += "io.lettuce"             % "lettuce-core"        % "6.2.2.RELEASE"
libraryDependencies += "com.github.pureconfig" %% "pureconfig"          % "0.17.2"
libraryDependencies += "ch.qos.logback"         % "logback-classic"     % "1.4.5"
libraryDependencies += "org.scalatest"         %% "scalatest"           % "3.2.15" % "test"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}
