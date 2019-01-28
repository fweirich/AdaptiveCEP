package adaptivecep.distributed

import java.time.temporal.TemporalUnit
import java.time.{Clock, Instant}
import java.util.concurrent.TimeUnit

import adaptivecep.data.Cost.Cost
import adaptivecep.data.Events._
import adaptivecep.simulation.ContinuousBoundedValue
import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.dispatch.{BoundedMessageQueueSemantics, RequiresMessageQueue}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

trait HostActorBase extends Actor with ActorLogging with RequiresMessageQueue[BoundedMessageQueueSemantics]{
  val cluster = Cluster(context.system)
  val interval = 1
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
  var throughputStartMap: Map[ActorRef, (Instant, Instant)] = Map.empty[ActorRef, (Instant, Instant)]
  var costs: Map[ActorRef, Cost] = Map.empty[ActorRef, Cost].withDefaultValue(Cost(FiniteDuration(0, TimeUnit.SECONDS), 100))

  case class HostProps(costs : Map[ActorRef, (ContinuousBoundedValue[Duration], ContinuousBoundedValue[Double])]) {
    def advance = HostProps(
      costs map { case (host, (latency, bandwidth)) => (host, (latency.advance, bandwidth.advance)) })
    def advanceLatency = HostProps(
      costs map { case (host, (latency, bandwidth)) => (host, (latency.advance, bandwidth)) })
    def advanceBandwidth = HostProps(
      costs map { case (host, (latency, bandwidth)) => (host, (latency, bandwidth.advance)) })
  }

  def reportCostsToNode(): Unit = {
    var result = Map.empty[ActorRef, Cost].withDefaultValue(Cost(FiniteDuration(0, TimeUnit.SECONDS), 100))
    hostToNodeMap.foreach(host =>
      if(hostPropsToMap.contains(host._1)){
        result += host._2 -> hostPropsToMap(host._1)
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
      min = 50, max = 1000,
      () => (40 - 80 * random.nextDouble, 1 + random.nextInt(10)))

    def apply() =
      template copy (value = 200 + 800 * random.nextDouble)
  }

  def hostPropsToMap: Map[ActorRef, Cost] = {
    hostProps.costs map { case (host, (latency, bandwidth)) => (host, Cost(latency.value, bandwidth.value)) }
  }

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
    startLatencyMonitoring()
  }
  override def postStop(): Unit = cluster.unsubscribe(self)

  def measureCosts() = {
    val now = clock.instant()
    for (neighbor <- neighbors){
      if(hostPropsToMap.contains(neighbor)) {
        neighbor ! StartThroughPutMeasurement(now)
        for (i <- Range(0, 100)) {
          context.system.scheduler.scheduleOnce(
            FiniteDuration(i, TimeUnit.MILLISECONDS),
            () => {
              neighbor ! TestEvent
            })
        }
        context.system.scheduler.scheduleOnce(
          FiniteDuration((bandwidth.template.max / hostPropsToMap(neighbor).bandwidth).toLong * 100, TimeUnit.MILLISECONDS),
          () => {
            hostPropsToMap(neighbor).bandwidth.toInt
            neighbor ! EndThroughPutMeasurement(now.plusMillis(100), hostPropsToMap(neighbor).bandwidth.toInt)
          })
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
      //println("Response", t)
      costs += sender() -> Cost(FiniteDuration(java.time.Duration.between(t, clock.instant()).dividedBy(2).toMillis, TimeUnit.MILLISECONDS), costs(sender()).bandwidth)
      //println(costs(sender()), hostPropsToMap(sender()))
    case StartThroughPutMeasurement(instant) =>
      throughputStartMap += sender() -> (instant, clock.instant())
      throughputMeasureMap += sender() -> 0
    case TestEvent =>
      throughputMeasureMap += sender() -> (throughputMeasureMap(sender()) + 1)
    case EndThroughPutMeasurement(instant, actual) =>
      val senderDiff = java.time.Duration.between(throughputStartMap(sender())._1, instant)
      val receiverDiff = java.time.Duration.between(throughputStartMap(sender())._2, clock.instant())
      val bandwidth = (senderDiff.toMillis.toDouble / receiverDiff.toMillis.toDouble) * ((1000 / senderDiff.toMillis) * throughputMeasureMap(sender()))
      println("(" + senderDiff.toMillis + "/" + receiverDiff.toMillis +") * (( 1000" +  "/" + senderDiff.toMillis + ") * " + throughputMeasureMap(sender()) + "))")
      println(bandwidth, actual)
      sender() ! ThroughPutResponse(bandwidth.toInt)
      throughputMeasureMap += sender() -> 0
    case ThroughPutResponse(r) =>
      //println("response", r)
      costs += sender() -> Cost(costs(sender()).duration, r*10)
      //println(costs(sender()), hostPropsToMap(sender()))
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
