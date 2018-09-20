package adaptivecep.distributed.centralized

import adaptivecep.distributed.HostActorBase

class HostActorCentralized extends HostActorBase {
/*
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
/*
  def delay(delay: Boolean): Unit = {
    node ! Delay(delay)
    this.delay = delay
  }
*/
  def receive = {
    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
      //context.system.actorSelection(member.address.toString + "/user/Host") ! LatencyRequest(clock.instant())
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)
    /*case LatencyRequest(time) =>
      if(sender() != self){
        if(delay) {
          sender() ! LatencyResponse(time.minusMillis(40))
        } else {
          sender() ! LatencyResponse(time)
        }
        //otherHosts += sender()
        //println(otherHosts)
      }*/
    /*case LatencyResponse(requestTime) =>
      if(sender() != self) {
        latencies += sender() -> FiniteDuration(java.time.Duration.between(requestTime, clock.instant).dividedBy(2).toMillis, TimeUnit.MILLISECONDS)
        //neighbors += sender()
      }*/
    case Neighbors(n, h)=>
      neighbors = n
      h.foreach(neighbor => simulatedCosts += neighbor -> (latency(), bandwidth()))
      hostProps = HostProps(simulatedCosts)
    /*case AllHosts => {
      context.system.actorSelection(self.path.address.toString + "/user/Placement") ! Hosts(neighbors)
      //println("sending Hosts", sender(), Hosts(neighbors + self))
    }*/
    /*case Delay(b) =>{
      if(sender() != self){
        delay(b)
        //println("delaying")
      }
    }*/
    case Node(actorRef) =>{
      node = actorRef
      //node ! Delay(delay)
    }
    case HostToNodeMap(m) =>
      hostToNodeMap = m
      println(hostToNodeMap)
      println("GotHostToNodeMap")
    case HostPropsRequest =>
      sender() ! HostPropsResponse(hostPropsToMap)
    case _ =>
  }*/
}