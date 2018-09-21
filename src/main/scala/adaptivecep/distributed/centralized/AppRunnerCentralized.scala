package adaptivecep.distributed.centralized

import java.io.File

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.distributed.operator.{ActiveOperator, NodeHost, Operator}
import adaptivecep.dsl.Dsl._
import adaptivecep.graph.qos._
import adaptivecep.publishers._
import akka.actor.{ActorRef, ActorSystem, Address, Deploy, Props}
import akka.remote.RemoteScope
import com.typesafe.config.ConfigFactory


object AppRunnerCentralized extends App {

  val file = new File("application.conf")
  val config = ConfigFactory.parseFile(file).withFallback(ConfigFactory.load()).resolve()
  var producers: Seq[Operator] = Seq.empty[Operator]
  val r = scala.util.Random
  var optimizeFor: String = "bandwidth"

  val actorSystem: ActorSystem = ActorSystem("ClusterSystem", config)

  override def main(args: Array[String]): Unit = {
    super.main(args)
    if (args.nonEmpty){
      optimizeFor = args(0)
    }
  }

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
          frequency > ratio(1.instances, 5.seconds) otherwise { (nodeData) => /*println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!")*/ }),
        slidingWindow(3.seconds),
        slidingWindow(3.seconds),
        bandwidth > dataRate(40.mbPerSecond) otherwise { nodeData => },
        latency < timespan(100.milliseconds) otherwise { (nodeData) => /*println(s"PROBLEM:\tEvents reach node `${nodeData.name}` too slowly!")*/ })

  val query3: Query4[Int, Int, Float, String] =
    stream[Int]("A")
      .join(
        stream[Int]("B"),
        slidingWindow(2.seconds),
        slidingWindow(2.seconds))
      .dropElem1()
      .selfJoin(
        tumblingWindow(1.instances),
        tumblingWindow(1.instances),
        frequency > ratio(3.instances, 5.seconds) otherwise { nodeData => println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!") },
        frequency < ratio(12.instances, 15.seconds) otherwise { nodeData => println(s"PROBLEM:\tNode `${nodeData.name}` emits too many events!") })
      .and(stream[Float]("C").and(stream[String]("D")), bandwidth > dataRate(40.mbPerSecond) otherwise { nodeData => })


  val address1 = Address("akka.tcp", "ClusterSystem", "18.219.222.126", 8000)
  val address2 = Address("akka.tcp", "ClusterSystem", sys.env("HOST2"), 8000)
  val address3 = Address("akka.tcp", "ClusterSystem", sys.env("HOST3"), 8000)
  val address4 = Address("akka.tcp", "ClusterSystem", sys.env("HOST4"), 8000)
  val address5 = Address("akka.tcp", "ClusterSystem", sys.env("HOST5"), 8000)
  val address6 = Address("akka.tcp", "ClusterSystem", sys.env("HOST6"), 8000)
  val address7 = Address("akka.tcp", "ClusterSystem", sys.env("HOST7"), 8000)
  val address8 = Address("akka.tcp", "ClusterSystem", sys.env("HOST8"), 8000)
  val address9 = Address("akka.tcp", "ClusterSystem", sys.env("HOST9"), 8000)
  val address10 = Address("akka.tcp", "ClusterSystem", sys.env("HOST10"), 8000)
  val address11 = Address("akka.tcp", "ClusterSystem", sys.env("HOST11"), 8000)
  val address12 = Address("akka.tcp", "ClusterSystem", sys.env("HOST12"), 8000)
  val address13 = Address("akka.tcp", "ClusterSystem", sys.env("HOST13"), 8000)
  val address14 = Address("akka.tcp", "ClusterSystem", sys.env("HOST14"), 8000)
  val address15 = Address("akka.tcp", "ClusterSystem", sys.env("HOST15"), 8000)
  val address16 = Address("akka.tcp", "ClusterSystem", sys.env("HOST16"), 8000)
  val address17 = Address("akka.tcp", "ClusterSystem", sys.env("HOST17"), 8000)
  val address18 = Address("akka.tcp", "ClusterSystem", sys.env("HOST18"), 8000)
  val address19 = Address("akka.tcp", "ClusterSystem", sys.env("HOST19"), 8000)
  val address20 = Address("akka.tcp", "ClusterSystem", sys.env("HOST20"), 8000)

  //LOCAL Setup
  /*
  val address1 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2551)
  val address2 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2552)
  val address3 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2553)
  val address4 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2554)
  val address5 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2555)
  val address6 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2556)
  val address7 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2557)
  val address8 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2558)
  val address9 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2559)
  val address10 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2560)
  val address11 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2561)
  */

  val host1: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address1))), "Host" + "1")
  val host2: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address2))), "Host" + "2")
  val host3: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address3))), "Host" + "3")
  val host4: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address4))), "Host" + "4")
  val host5: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address5))), "Host" + "5")
  val host6: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address6))), "Host" + "6")
  val host7: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address7))), "Host" + "7")
  val host8: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address8))), "Host" + "8")
  val host9: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address9))), "Host" + "9")
  val host10: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address10))), "Host" + "10")
  val host11: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address11))), "Host" + "11")
  val host12: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address12))), "Host" + "12")
  val host13: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address13))), "Host" + "13")
  val host14: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address14))), "Host" + "14")
  val host15: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address15))), "Host" + "15")
  val host16: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address16))), "Host" + "16")
  val host17: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address17))), "Host" + "17")
  val host18: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address18))), "Host" + "18")
  val host19: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address19))), "Host" + "19")
  val host20: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address20))), "Host" + "20")

  val hosts: Set[ActorRef] = Set(host1, host2, host3, host4, host5, host6, host7, host8, host9, host10, host11,
    host12, host13, host14, host15, host16, host17, host18, host19, host20)

  hosts.foreach(host => host ! Neighbors(hosts, hosts))
/*
  val neighborsOfHost1: Set[ActorRef] = Set(host5, host6, host7, host8, host9, host11/*, host12, host16*/)
  val neighborsOfHost2: Set[ActorRef] = Set(host5, host6, host7, host8, host9, host11/*, host16*/)
  val neighborsOfHost3: Set[ActorRef] = Set(host5, host6, host7, host8, host9, host10, host11/*, host16*/)
  val neighborsOfHost4: Set[ActorRef] = Set(host5, host6, host7, host8, host9, host11/*, host16*/)
  val neighborsOfHost5: Set[ActorRef] = Set(host1, host2, host3, host4, host6, host9, host11/*, host12, host13, host16*/)
  val neighborsOfHost6: Set[ActorRef] = Set(host1, host2, host3, host4, host5, host7, host9, host10, host11/*, host16*/)
  val neighborsOfHost7: Set[ActorRef] = Set(host1, host2, host3, host4, host6, host8, host9, host10, host11/*, host14, host16*/)
  val neighborsOfHost8: Set[ActorRef] = Set(host1, host2, host3, host4, host7, host9, host10, host11/*, host14, host16*/)
  val neighborsOfHost9: Set[ActorRef] = Set(host1, host2, host5, host6, host7, host8, host10, host11/*, host12, host13, host14, host16*/)
  val neighborsOfHost10: Set[ActorRef] = Set(host5, host6, host7, host8, host9, host11/*, host12, host13, host14, host15, host16*/)
  val neighborsOfHost11: Set[ActorRef] = Set(host5, host6, host7, host8, host9, host10, host11/*, host13, host14, host16*/)
  val neighborsOfHost12: Set[ActorRef] = Set(host1, host5, host9, host10, host11/*, host13, host16*/)
  val neighborsOfHost13: Set[ActorRef] = Set(host5, host8, host9, host10, host11 /*host12, host13, host14, host15, host16*/)
  val neighborsOfHost14: Set[ActorRef] = Set(host7, host8, host9, host10, host11/*, host13, host14, host15, host16*/)
  val neighborsOfHost15: Set[ActorRef] = Set(host10 /*host13, host14, host16*/)
  val neighborsOfHost16: Set[ActorRef] = Set(host10 /*host13, host14, host15*/)
*/
  /*
  host1 ! Neighbors(neighborsOfHost1, hosts)
  host2 ! Neighbors(neighborsOfHost2, hosts)
  host3 ! Neighbors(neighborsOfHost3, hosts)
  host4 ! Neighbors(neighborsOfHost4, hosts)
  host5 ! Neighbors(neighborsOfHost5, hosts)
  host6 ! Neighbors(neighborsOfHost6, hosts)
  host7 ! Neighbors(neighborsOfHost7, hosts)
  host8 ! Neighbors(neighborsOfHost8, hosts)
  host9 ! Neighbors(neighborsOfHost9, hosts)
  host10 ! Neighbors(neighborsOfHost10, hosts)
  host11 ! Neighbors(neighborsOfHost11, hosts)
  host12 ! Neighbors(neighborsOfHost12)
  host13 ! Neighbors(neighborsOfHost13)
  host14 ! Neighbors(neighborsOfHost14)
  host15 ! Neighbors(neighborsOfHost15)
  host16 ! Neighbors(neighborsOfHost16)*/

  val publisherA: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id))).withDeploy(Deploy(scope = RemoteScope(address1))),             "A")
  val publisherB: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id * 2))).withDeploy(Deploy(scope = RemoteScope(address2))),         "B")
  val publisherC: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id.toFloat))).withDeploy(Deploy(scope = RemoteScope(address3))),     "C")
  val publisherD: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(s"String($id)"))).withDeploy(Deploy(scope = RemoteScope(address4))), "D")

  val operatorA = ActiveOperator(NodeHost(host1), null, Seq.empty[Operator])
  val operatorB = ActiveOperator(NodeHost(host2), null, Seq.empty[Operator])
  val operatorC = ActiveOperator(NodeHost(host3), null, Seq.empty[Operator])
  val operatorD = ActiveOperator(NodeHost(host4), null, Seq.empty[Operator])


  //val delayableHosts: Seq[ActorRef] = Seq(host2, host3, host4, host5, host6, host7, host8, host9, host10/*, host11, host12, host13, host14, host15*/)

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

  Thread.sleep(3000)

  val placement: ActorRef = actorSystem.actorOf(Props(PlacementActorCentralized(actorSystem,
    query3,
    publishers, publisherOperators,
    AverageFrequencyMonitorFactory(interval = 15, logging = false),
    PathLatencyMonitorFactory(interval =  2, logging = false),
    PathBandwidthMonitorFactory(interval = 2, logging = false),NodeHost(host20), hosts, optimizeFor)), "Placement")

  placement ! InitializeQuery
  Thread.sleep(10000)
  placement ! Start

}
