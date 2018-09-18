package adaptivecep.distributed

import java.time.Clock
import java.util.concurrent.TimeUnit

import adaptivecep.data.Cost.Cost
import adaptivecep.data.Events._
import adaptivecep.simulation.ContinuousBoundedValue
import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

trait HostActorBase extends Actor with ActorLogging{
  val cluster = Cluster(context.system)
  val interval = 2
  var neighbors: Set[ActorRef] = Set.empty[ActorRef]
  var node: ActorRef = self
  var delay: Boolean = false
  val clock: Clock = Clock.systemDefaultZone
  var latencies: Map[ActorRef, scala.concurrent.duration.Duration] = Map.empty[ActorRef, scala.concurrent.duration.Duration]
  val random: Random = new Random(0)
  var simulatedCosts: Map[ActorRef, (ContinuousBoundedValue[Duration], ContinuousBoundedValue[Double])] =
    Map.empty[ActorRef, (ContinuousBoundedValue[Duration], ContinuousBoundedValue[Double])]
  var hostProps: HostProps = HostProps(simulatedCosts)
  var hostToNodeMap: Map[ActorRef, ActorRef] = Map.empty[ActorRef, ActorRef]

  case class HostProps(costs : Map[ActorRef, (ContinuousBoundedValue[Duration], ContinuousBoundedValue[Double])]) {
    def advance = HostProps(
      costs map { case (host, (latency, bandwidth)) => (host, (latency.advance, bandwidth.advance)) })
  }

  def reportCostsToNode(): Unit = {
    var result = Map.empty[ActorRef, Cost]
    val map = hostPropsToMap
    hostToNodeMap.foreach(host => if(map.contains(host._1)){result += host._2 -> map(host._1)})
    node ! HostPropsResponse(result)
  }

  object latency {
    implicit val addDuration: (Duration, Duration) => Duration = _ + _

    val template = ContinuousBoundedValue[Duration](
      Duration.Undefined,
      min = 2.millis, max = 100.millis,
      () => (10.millis - 30.milli * random.nextDouble, 1 + random.nextInt(10)))

    def apply() =
      template copy (value = 2.milli + 98.millis * random.nextDouble)
  }

  object bandwidth {
    implicit val addDouble: (Double, Double) => Double = _ + _

    val template = ContinuousBoundedValue[Double](
      0,
      min = 5, max = 100,
      () => (10 - 30 * random.nextDouble, 1 + random.nextInt(10)))

    def apply() =
      template copy (value = 5 + 95* random.nextDouble)
  }

  def hostPropsToMap: Map[ActorRef, Cost] = {
    hostProps.costs map { case (host, (latency, bandwidth)) => (host, Cost(latency.value, bandwidth.value)) }
  }

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }
  override def postStop(): Unit = cluster.unsubscribe(self)

  def startLatencyMonitoring(): Unit = context.system.scheduler.schedule(
    initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
    interval = FiniteDuration(interval, TimeUnit.SECONDS),
    runnable = () => {
      hostProps = hostProps.advance
      reportCostsToNode()
    })

  startLatencyMonitoring()

  def receive = {
    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
    //context.system.actorSelection(member.address.toString + "/user/Host") ! LatencyRequest(clock.instant())
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)
    case Neighbors(n, h)=>
      neighbors = n
      h.foreach(neighbor => simulatedCosts += neighbor -> (latency(), bandwidth()))
      hostProps = HostProps(simulatedCosts)
    case Node(actorRef) =>{
      node = actorRef
    }
    case HostToNodeMap(m) =>
      hostToNodeMap = m
      println(hostToNodeMap)
      println("GotHostToNodeMap")
    case HostPropsRequest =>
      sender() ! HostPropsResponse(hostPropsToMap)
    case _ =>
  }
}
