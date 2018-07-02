package adaptivecep.distributed

import java.io.File

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.dsl.Dsl._
import adaptivecep.graph.qos._
import adaptivecep.publishers._
import akka.actor.{ActorRef, ActorSystem, Address, Deploy, Props}
import com.typesafe.config.ConfigFactory


object AppRunner extends App {

  val file = new File("application.conf")
  val config = ConfigFactory.parseFile(file).withFallback(ConfigFactory.load()).resolve()

  val actorSystem: ActorSystem = ActorSystem("ClusterSystem", config)

  val publisherA: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id))),             "A")
  val publisherB: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id * 2))),         "B")
  val publisherC: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id.toFloat))),     "C")
  val publisherD: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(s"String($id)"))), "D")

  val publishers: Map[String, ActorRef] = Map(
    "A" -> publisherA,
    "B" -> publisherB,
    "C" -> publisherC,
    "D" -> publisherD)

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

  val test: ActorRef = actorSystem.actorOf(Props(PlacementActor(actorSystem,
    query1,
    publishers,
    AverageFrequencyMonitorFactory(interval = 15, logging = true),
    PathLatencyMonitorFactory(interval =  5, logging = true) )), "Placement")

  test ! InitializeQuery
  //Thread.sleep(10000)
  //test ! "Move"

  /*
  val graph: ActorRef = GraphFactory.create(
    actorSystem =             actorSystem,
    query =                   query1, // Alternatively: `query2`
    publishers =              publishers,
    frequencyMonitorFactory = AverageFrequencyMonitorFactory  (interval = 15, logging = true),
    latencyMonitorFactory =   PathLatencyMonitorFactory       (interval =  5, logging = true),
    createdCallback =         () => println("STATUS:\t\tGraph has been created."))(
    eventCallback =           {
      // Callback for `query1`:
      case (Left(i1), Left(i2), Left(f)) => println(s"COMPLEX EVENT:\tEvent3($i1,$i2,$f)")
      case (Right(s), _, _)              => println(s"COMPLEX EVENT:\tEvent1($s)")
      // Callback for `query2`:
      // case (i1, i2, f, s)             => println(s"COMPLEX EVENT:\tEvent4($i1, $i2, $f,$s)")
      // This is necessary to avoid warnings about non-exhaustive `match`:
      case _                             =>
    })
    */
}
