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
  var optimizeFor: String = "latency"
  var neighbors: Set[ActorRef] = Set.empty[ActorRef]
  var node: Option[ActorRef] = Some(self)
  val clock: Clock = Clock.systemDefaultZone
  var latencies: Map[ActorRef, scala.concurrent.duration.Duration] = Map.empty[ActorRef, scala.concurrent.duration.Duration]
  val random: Random = new Random(clock.millis())
  var simulatedCosts: Map[ActorRef, (ContinuousBoundedValue[Duration], ContinuousBoundedValue[Double])] =
    Map.empty[ActorRef, (ContinuousBoundedValue[Duration], ContinuousBoundedValue[Double])]
  var hostProps: HostProps = HostProps(simulatedCosts)
  var hostToNodeMap: Map[ActorRef, ActorRef] = Map.empty[ActorRef, ActorRef]
  var throughputMeasureMap: Map[ActorRef, Int] = Map.empty[ActorRef, Int] withDefaultValue(0)
  var costs: Map[ActorRef, Cost] = Map.empty[ActorRef, Cost].withDefaultValue(Cost(Duration.Zero, 100))

  case class HostProps(costs : Map[ActorRef, (ContinuousBoundedValue[Duration], ContinuousBoundedValue[Double])]) {
    def advance = HostProps(
      costs map { case (host, (latency, bandwidth)) => (host, (latency.advance, bandwidth.advance)) })
    def advanceLatency = HostProps(
      costs map { case (host, (latency, bandwidth)) => (host, (latency.advance, bandwidth)) })
    def advanceBandwidth = HostProps(
      costs map { case (host, (latency, bandwidth)) => (host, (latency, bandwidth.advance)) })
  }

  def reportCostsToNode(): Unit = {
    var result = Map.empty[ActorRef, Cost].withDefaultValue(Cost(Duration.Zero, 100))
    hostToNodeMap.foreach(host =>
      if(costs.contains(host._1)){
        result += host._2 -> costs(host._1)
      }
    )
    if(node.isDefined){
      node.get ! HostPropsResponse(result)
    }
  }

  object latency {
    implicit val addDuration: (Duration, Duration) => Duration = _ + _

    val template = ContinuousBoundedValue[Duration](
      Duration.Undefined,
      min = 2.millis, max = 100.millis,
      () => (2.millis - 4.milli * random.nextDouble, 1 + random.nextInt(10)))

    def apply() =
      template copy (value = 5.milli + 95.millis * random.nextDouble)
  }

  object bandwidth {
    implicit val addDouble: (Double, Double) => Double = _ + _

    val template = ContinuousBoundedValue[Double](
      0,
      min = 5, max = 100,
      () => (2 - 4 * random.nextDouble, 1 + random.nextInt(10)))

    def apply() =
      template copy (value = 20 + 80* random.nextDouble)
  }

  def hostPropsToMap: Map[ActorRef, Cost] = {
    hostProps.costs map { case (host, (latency, bandwidth)) => (host, Cost(latency.value, bandwidth.value)) }
  }

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }
  override def postStop(): Unit = cluster.unsubscribe(self)

  def measureCosts() = {
    for (neighbor <- neighbors){
      if(hostPropsToMap.contains(neighbor)) {
        neighbor ! StartThroughPutMeasurement
        for (i <- Range(0, hostPropsToMap(neighbor).bandwidth.toInt)) {
          context.system.scheduler.scheduleOnce(
            FiniteDuration(i*10, TimeUnit.MILLISECONDS),
            () => {
              neighbor ! TestEvent
            })
        }
        context.system.scheduler.scheduleOnce(
          FiniteDuration(1000, TimeUnit.MILLISECONDS),
          () => {
            neighbor ! EndThroughPutMeasurement
          })
        val now = clock.instant()
        if (hostPropsToMap.contains(neighbor)) {
          context.system.scheduler.scheduleOnce(
            FiniteDuration(hostPropsToMap(neighbor).duration.toMillis * 2, TimeUnit.MILLISECONDS),
            () => {
              neighbor ! LatencyRequest(now)
            })
        } else {
          neighbor ! LatencyRequest(now)
        }
      }
    }
  }

  def startLatencyMonitoring(): Unit = context.system.scheduler.schedule(
    initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
    interval = FiniteDuration(interval, TimeUnit.SECONDS),
    runnable = () => {
      if(optimizeFor == "latency"){
        hostProps = hostProps.advanceLatency
      } else if (optimizeFor == "bandwidth") {
        hostProps = hostProps.advanceBandwidth
      } else {
        hostProps = hostProps.advance
      }
      measureCosts()
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
      node = Some(actorRef)
    }
    case OptimizeFor(o) => optimizeFor = o
    case HostToNodeMap(m) =>
      hostToNodeMap = m
    case HostPropsRequest =>
      sender() ! HostPropsResponse(costs)
    case LatencyRequest(t)=>
      sender() ! LatencyResponse(t)
    case LatencyResponse(t) =>
      println("Response", t)
      costs += sender() -> Cost(FiniteDuration(java.time.Duration.between(t, clock.instant()).toMillis, TimeUnit.MILLISECONDS), costs(sender()).bandwidth)
    case StartThroughPutMeasurement =>
    case TestEvent => throughputMeasureMap += sender() -> (throughputMeasureMap(sender()) + 1)
    case EndThroughPutMeasurement =>
      send(sender(), ThroughPutResponse(throughputMeasureMap(sender())))
      throughputMeasureMap += sender() -> 0
    case ThroughPutResponse(r) =>
      println("response", r)
      costs += sender() -> Cost(costs(sender()).duration, r)
    case _ =>
  }

  def send(receiver: ActorRef, message: Any): Unit ={
    if(hostPropsToMap.contains(receiver)){
      context.system.scheduler.scheduleOnce(
        FiniteDuration(hostPropsToMap(receiver).duration.toMillis, TimeUnit.MILLISECONDS),
        () => {receiver ! message})
    }
    else {
      receiver ! message
    }
  }
}
