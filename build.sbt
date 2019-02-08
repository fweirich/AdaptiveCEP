name := "AdaptiveCEP"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.12.1"

resolvers += Resolver.bintrayRepo("stg-tud", "maven")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor"   % "2.4.16",
  "com.typesafe.akka" %% "akka-testkit" % "2.4.16"  % "test",
  "com.espertech"     %  "esper"        % "5.5.0",
  "org.scalatest"     %% "scalatest"    % "3.0.1"   % "test",
  "de.tuda.stg" %% "rescala" % "0.24.0",
  "com.typesafe.akka" %% "akka-cluster" % "2.5.12",
  "com.typesafe.akka" %% "akka-cluster-tools" % "2.5.13"
)
