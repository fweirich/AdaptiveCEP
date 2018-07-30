package adaptivecep.distributed.random

import java.io.File

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.distributed.fixed.PlacementActorFixed
import adaptivecep.distributed.{ActiveOperator, NodeHost, Operator}
import adaptivecep.dsl.Dsl._
import adaptivecep.graph.qos._
import adaptivecep.publishers._
import akka.actor.{ActorRef, ActorSystem, Deploy, Props}
import akka.remote
import com.typesafe.config.ConfigFactory


object AppRunner extends App {

  val file = new File("application.conf")
  val config = ConfigFactory.parseFile(file).withFallback(ConfigFactory.load()).resolve()
  var producers: Seq[Operator] = Seq.empty[Operator]

  val actorSystem: ActorSystem = ActorSystem("ClusterSystem", config)
  val host: ActorRef = actorSystem.actorOf(Props[HostActor], "Host")

  val publisherA: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id))).withDeploy(Deploy(scope = remote.RemoteScope(host.path.address))),             "A")
  val publisherB: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id * 2))).withDeploy(Deploy(scope = remote.RemoteScope(host.path.address))),         "B")
  val publisherC: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id.toFloat))).withDeploy(Deploy(scope = remote.RemoteScope(host.path.address))),     "C")
  val publisherD: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(s"String($id)"))).withDeploy(Deploy(scope = remote.RemoteScope(host.path.address))), "D")

  val operatorA = ActiveOperator(NodeHost(host), null, Seq.empty[Operator])
  val operatorB = ActiveOperator(NodeHost(host), null, Seq.empty[Operator])
  val operatorC = ActiveOperator(NodeHost(host), null, Seq.empty[Operator])
  val operatorD = ActiveOperator(NodeHost(host), null, Seq.empty[Operator])

  val publishers: Map[String, ActorRef] = Map(
    "A" -> publisherA,
    "B" -> publisherB,
    "C" -> publisherC,
    "D" -> publisherD)

  val publisherOperators: Map[String, Operator] = Map(
    "A" -> operatorA,
    "B" -> operatorB,
    "C" -> operatorC,
    "D" -> operatorD)

  val query1: Query3[Either[Int, String], Either[Int, X], Either[Float, X]] =
    stream[Int]("A")
      .join(
        stream[Int]("B"),
        slidingWindow(2.seconds),
        slidingWindow(2.seconds))
      .where(_ < _)
      .dropElem1(
        latency < timespan(1.milliseconds) otherwise { nodeData => println(s"PROBLEM:\tEvents reach node `${nodeData.name}` too slowly!") })
      .selfJoin(
        tumblingWindow(1.instances),
        tumblingWindow(1.instances),
        frequency > ratio( 3.instances,  5.seconds) otherwise { nodeData => println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!") },
        frequency < ratio(12.instances, 15.seconds) otherwise { nodeData => println(s"PROBLEM:\tNode `${nodeData.name}` emits too many events!") })
      .and(stream[Float]("C"))
      .or(stream[String]("D"))

  val query2: Query4[Int, Int, Float, String] =
    stream[Int]("A")
      .and(stream[Int]("B"))
      .join(
        sequence(
          nStream[Float]("C") -> nStream[String]("D"),
          frequency > ratio(1.instances, 5.seconds) otherwise { (nodeData) => println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!") }),
        slidingWindow(3.seconds),
        slidingWindow(3.seconds),
        latency < timespan(1.milliseconds) otherwise { (nodeData) => println(s"PROBLEM:\tEvents reach node `${nodeData.name}` too slowly!") })

  Thread.sleep(3000)

  val placement: ActorRef = actorSystem.actorOf(Props(PlacementActor(actorSystem,
    query1,
    publishers, publisherOperators,
    AverageFrequencyMonitorFactory(interval = 15, logging = true),
    PathLatencyMonitorFactory(interval =  5, logging = true), NodeHost(host))), "Placement")

  placement ! InitializeQuery
  Thread.sleep(10000)
  placement ! Start
}
