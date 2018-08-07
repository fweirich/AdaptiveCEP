package adaptivecep.distributed.fixed

import java.io.File

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.distributed.random.{HostActor, PlacementActor}
import adaptivecep.distributed.{ActiveOperator, NodeHost, Operator}
import adaptivecep.dsl.Dsl._
import adaptivecep.graph.qos._
import adaptivecep.publishers._
import akka.actor.{ActorRef, ActorSystem, Address, Deploy, Props}
import akka.remote.RemoteScope
import com.typesafe.config.ConfigFactory


object AppRunnerFixed extends App {

  val file = new File("application.conf")
  val config = ConfigFactory.parseFile(file).withFallback(ConfigFactory.load()).resolve()
  var producers: Seq[Operator] = Seq.empty[Operator]
  val r = scala.util.Random

  val actorSystem: ActorSystem = ActorSystem("ClusterSystem", config)
  //val consumerHost: ActorRef = actorSystem.actorOf(Props[HostActor], "Host")

  /*
  val publisherAddress1 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2551)
  val publisherAddress2 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2552)
  val publisherAddress3 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2553)
  val publisherAddress4 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2554)
  val address1 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2555)
  val address2 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2556)
  val address3 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2557)
  val address4 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2558)
  val address5 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2559)
  val address6 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2560)
  val address7 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2561)
  val address8 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2562)
  val address9 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2563)
  val address10 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2564)
  val address11 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2565)
  val address12 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2566)

  val host1: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(publisherAddress1))), "Host" + "-1")
  val host2: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(publisherAddress2))), "Host" + "-2")
  val host3: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(publisherAddress3))), "Host" + "-3")
  val host4: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(publisherAddress4))), "Host" + "-4")
  val host5: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address1))), "Host" + "-5")
  val host6: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address2))), "Host" + "-6")
  val host7: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address3))), "Host" + "-7")
  val host8: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address4))), "Host" + "-8")
  val host9: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address5))), "Host" + "-9")
  val host10: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address6))), "Host" + "-10")
  val host11: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address7))), "Host" + "-11")
  val host12: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address8))), "Host" + "-12")
  val host13: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address9))), "Host" + "-13")
  val host14: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address10))), "Host" + "-14")
  val host15: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address11))), "Host" + "-15")
  val host16: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address12))), "Host" + "-16")

  val neighborsOfHost1: Set[ActorRef] = Set(host2, host5, host6, host8, host9, host11, host13)
  val neighborsOfHost2: Set[ActorRef] = Set(host1, host3, host5, host6, host9, host11)
  val neighborsOfHost3: Set[ActorRef] = Set(host2, host4, host6, host7, host9, host10, host12)
  val neighborsOfHost4: Set[ActorRef] = Set(host3, host6, host7, host10, host12)
  val neighborsOfHost5: Set[ActorRef] = Set(host1, host2, host6, host8, host9, host11, host13, host14)
  val neighborsOfHost6: Set[ActorRef] = Set(host2, host3, host5, host9, host10, host11, host14)
  val neighborsOfHost7: Set[ActorRef] = Set(host3, host4, host6, host10, host12, host15)
  val neighborsOfHost8: Set[ActorRef] = Set(host1, host2, host5, host6, host9, host11, host13)
  val neighborsOfHost9: Set[ActorRef] = Set(host1, host2, host3, host5, host6, host7, host11, host14, host15)
  val neighborsOfHost10: Set[ActorRef] = Set(host3, host6, host7, host9, host12, host14, host15)
  val neighborsOfHost11: Set[ActorRef] = Set(host1, host2, host5, host6, host8, host9, host13, host14)
  val neighborsOfHost12: Set[ActorRef] = Set(host3, host7, host10, host14, host15, host16)
  val neighborsOfHost13: Set[ActorRef] = Set(host1, host5, host6, host8, host9, host11, host14)
  val neighborsOfHost14: Set[ActorRef] = Set(host5, host6, host9, host10, host11, host15)
  val neighborsOfHost15: Set[ActorRef] = Set(host7, host9, host10, host12, host14)
  val neighborsOfHost16: Set[ActorRef] = Set(host7, host10, host12, host14, host15)

  host1 ! Neighbors(neighborsOfHost1)
  host2 ! Neighbors(neighborsOfHost2)
  host3 ! Neighbors(neighborsOfHost3)
  host4 ! Neighbors(neighborsOfHost4)
  host5 ! Neighbors(neighborsOfHost5)
  host6 ! Neighbors(neighborsOfHost6)
  host7 ! Neighbors(neighborsOfHost7)
  host8 ! Neighbors(neighborsOfHost8)
  host9 ! Neighbors(neighborsOfHost9)
  host10 ! Neighbors(neighborsOfHost10)
  host11 ! Neighbors(neighborsOfHost11)
  host12 ! Neighbors(neighborsOfHost12)
  host13 ! Neighbors(neighborsOfHost13)
  host14 ! Neighbors(neighborsOfHost14)
  host15 ! Neighbors(neighborsOfHost15)
  host16 ! Neighbors(neighborsOfHost16)
  */
  val globalIP: String = "[2a02:908:181:7140:69c9:9a2f:55ff:458e]"

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
  /*val address12 = Address("akka.tcp", "ClusterSystem", "[2600:1f16:948:9b01:dbe6:4d20:b19e:5fcf]", 8000)
  val address13 = Address("akka.tcp", "ClusterSystem", "[2600:1f16:948:9b01:a5f7:96f6:f59:7752]", 8000)
  val address14 = Address("akka.tcp", "ClusterSystem", "[2600:1f16:948:9b01:fb5e:e067:3fc7:89a6]", 8000)
  val address15 = Address("akka.tcp", "ClusterSystem", "[2600:1f16:948:9b01:f651:2a09:88b:9d1d]", 8000)
  val address16 = Address("akka.tcp", "ClusterSystem", globalIP, 8000)*/
/*
  val alternativeAddress5 = Address("akka.tcp", "ClusterSystem", globalIP, 8001)
  val alternativeAddress6 = Address("akka.tcp", "ClusterSystem", globalIP, 8002)
  val alternativeAddress7 = Address("akka.tcp", "ClusterSystem", globalIP, 8003)
  val alternativeAddress8 = Address("akka.tcp", "ClusterSystem", globalIP, 8004)
  val alternativeAddress9 = Address("akka.tcp", "ClusterSystem", globalIP, 8005)
  val alternativeAddress10 = Address("akka.tcp", "ClusterSystem", globalIP, 8006)
  val alternativeAddress11 = Address("akka.tcp", "ClusterSystem", globalIP, 8007)
  val alternativeAddress12 = Address("akka.tcp", "ClusterSystem", globalIP, 8008)
  val alternativeAddress13 = Address("akka.tcp", "ClusterSystem", globalIP, 8009)
  val alternativeAddress14 = Address("akka.tcp", "ClusterSystem", globalIP, 8010)
  val alternativeAddress15 = Address("akka.tcp", "ClusterSystem", globalIP, 8011)
*/
  val host1: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address1))), "Host" + "1")
  val host2: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address2))), "Host" + "2")
  val host3: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address3))), "Host" + "3")
  val host4: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address4))), "Host" + "4")
  val host5: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address5))), "Host" + "5")
  val host6: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address6))), "Host" + "6")
  val host7: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address7))), "Host" + "7")
  val host8: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address8))), "Host" + "8")
  val host9: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address9))), "Host" + "9")
  val host10: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address10))), "Host" + "10")
  val host11: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address11))), "Host" + "11")
  /*val host12: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address12))), "Host" + "12")
  val host13: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address13))), "Host" + "13")
  val host14: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address14))), "Host" + "14")
  val host15: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address15))), "Host" + "15")
  val host16: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(address16))), "Host" + "16")*/
/*
  val alternativeHost5: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(alternativeAddress5))), "Host" + "-1")
  val alternativeHost6: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(alternativeAddress6))), "Host" + "-2")
  val alternativeHost7: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(alternativeAddress7))), "Host" + "-3")
  val alternativeHost8: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(alternativeAddress8))), "Host" + "-4")
  val alternativeHost9: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(alternativeAddress9))), "Host" + "-5")
  val alternativeHost10: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(alternativeAddress10))), "Host" + "-6")
  val alternativeHost11: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(alternativeAddress11))), "Host" + "-7")
  val alternativeHost12: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(alternativeAddress12))), "Host" + "-8")
  val alternativeHost13: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(alternativeAddress13))), "Host" + "-9")
  val alternativeHost14: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(alternativeAddress14))), "Host" + "-10")
  val alternativeHost15: ActorRef = actorSystem.actorOf(Props[HostActorFixed].withDeploy(Deploy(scope = RemoteScope(alternativeAddress15))), "Host" + "-11")
  */

  val neighborsOfHost1: Set[ActorRef] = Set(host5, host6, host7, host8, host9/*, host12, host16*/)
  val neighborsOfHost2: Set[ActorRef] = Set(host5, host6, host7, host8, host9/*, host16*/)
  val neighborsOfHost3: Set[ActorRef] = Set(host5, host6, host7, host8, host9, host10, host11/*, host16*/)
  val neighborsOfHost4: Set[ActorRef] = Set(host5, host6, host7, host8, host9, host11/*, host16*/)
  val neighborsOfHost5: Set[ActorRef] = Set(host1, host2, host3, host4, host6, host9/*, host12, host13, host16*/)
  val neighborsOfHost6: Set[ActorRef] = Set(host1, host2, host3, host4, host5, host7, host9, host10, host11/*, host16*/)
  val neighborsOfHost7: Set[ActorRef] = Set(host1, host2, host3, host4, host6, host8, host9, host10, host11/*, host14, host16*/)
  val neighborsOfHost8: Set[ActorRef] = Set(host1, host2, host3, host4, host7, host9, host10, host11/*, host14, host16*/)
  val neighborsOfHost9: Set[ActorRef] = Set(host1, host2, host5, host6, host7, host8, host10, host11/*, host12, host13, host14, host16*/)
  val neighborsOfHost10: Set[ActorRef] = Set(host5, host6, host7, host8, host9, host11/*, host12, host13, host14, host15, host16*/)
  val neighborsOfHost11: Set[ActorRef] = Set(host5, host6, host7, host8, host9, host10/*, host13, host14, host16*/)
  val neighborsOfHost12: Set[ActorRef] = Set(host1, host5, host9, host10/*, host13, host16*/)
  val neighborsOfHost13: Set[ActorRef] = Set(host5, host8, host9, host10, host11 /*host12, host13, host14, host15, host16*/)
  val neighborsOfHost14: Set[ActorRef] = Set(host7, host8, host9, host10, host11/*, host13, host14, host15, host16*/)
  val neighborsOfHost15: Set[ActorRef] = Set(host10 /*host13, host14, host16*/)
  val neighborsOfHost16: Set[ActorRef] = Set(host10 /*host13, host14, host15*/)

  host1 ! Neighbors(neighborsOfHost1)
  host2 ! Neighbors(neighborsOfHost2)
  host3 ! Neighbors(neighborsOfHost3)
  host4 ! Neighbors(neighborsOfHost4)
  host5 ! Neighbors(neighborsOfHost5)
  host6 ! Neighbors(neighborsOfHost6)
  host7 ! Neighbors(neighborsOfHost7)
  host8 ! Neighbors(neighborsOfHost8)
  host9 ! Neighbors(neighborsOfHost9)
  host10 ! Neighbors(neighborsOfHost10)
  host11 ! Neighbors(neighborsOfHost11)
  /*host12 ! Neighbors(neighborsOfHost12)
  host13 ! Neighbors(neighborsOfHost13)
  host14 ! Neighbors(neighborsOfHost14)
  host15 ! Neighbors(neighborsOfHost15)
  host16 ! Neighbors(neighborsOfHost16)*/

  val publisherA: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id))).withDeploy(Deploy(scope = RemoteScope(address1))),             "A")
  val publisherB: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id * 2))).withDeploy(Deploy(scope = RemoteScope(address1))),         "B")
  val publisherC: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id.toFloat))).withDeploy(Deploy(scope = RemoteScope(address1))),     "C")
  val publisherD: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(s"String($id)"))).withDeploy(Deploy(scope = RemoteScope(address1))), "D")

  val operatorA = ActiveOperator(NodeHost(host1), null, Seq.empty[Operator])
  val operatorB = ActiveOperator(NodeHost(host2), null, Seq.empty[Operator])
  val operatorC = ActiveOperator(NodeHost(host3), null, Seq.empty[Operator])
  val operatorD = ActiveOperator(NodeHost(host4), null, Seq.empty[Operator])

  val hosts: Set[ActorRef] = Set(host1, host2, host3, host4, host5, host6, host7, host8, host9, host10, host11/*, host12, host13, host14, host15, host16*/)
  val delayableHosts: Seq[ActorRef] = Seq(host2, host3, host4, host5, host6, host7, host8, host9, host10/*, host11, host12, host13, host14, host15*/)

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
          frequency > ratio(1.instances, 5.seconds) otherwise { (nodeData) => /*println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!")*/ }),
        slidingWindow(3.seconds),
        slidingWindow(3.seconds),
        latency < timespan(10.milliseconds) otherwise { (nodeData) => /*println(s"PROBLEM:\tEvents reach node `${nodeData.name}` too slowly!")*/ })

  Thread.sleep(3000)

  val placement: ActorRef = actorSystem.actorOf(Props(PlacementActorFixed(actorSystem,
    query2,
    publishers, publisherOperators,
    AverageFrequencyMonitorFactory(interval = 15, logging = false),
    PathLatencyMonitorFactory(interval =  2, logging = false), NodeHost(host11), hosts)), "Placement")

  placement ! InitializeQuery
  Thread.sleep(10000)
  placement ! Start

  var delayedHosts: Seq[ActorRef] = Seq.empty[ActorRef]

  while (true){
    Thread.sleep(30000)
    //println("delaying Hosts")
    delayedHosts.foreach(host => host ! Delay(false))
    delayedHosts = Seq.empty[ActorRef]
    var delayIds: Set[Int] = Set.empty[Int]

    while (delayIds.size < 4){
      val temp = r.nextInt(delayableHosts.size)
      if(!delayIds.contains(temp)){
        delayIds += temp
      }
    }
    delayIds.foreach(index =>
      delayedHosts = delayedHosts :+ delayableHosts(index)
    )
    //println(delayIds)
    //println(delayedHosts)
    delayedHosts.foreach(host => host ! Delay(true))
  }

}

