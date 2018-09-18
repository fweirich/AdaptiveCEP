package adaptivecep.distributed.greedy

import java.time.Clock
import java.util.concurrent.TimeUnit

import adaptivecep.data.Cost.Cost
import adaptivecep.data.Events._
import adaptivecep.distributed.operator.{ActiveOperator, NodeHost, TentativeOperator}
import adaptivecep.distributed.{NodeHost, TentativeOperator, operator}
import adaptivecep.graph.qos.ChildLatencyResponse
import adaptivecep.simulation.ContinuousBoundedValue
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Deploy, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.remote

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class HostActorGreedy extends Actor with ActorLogging {

  val cluster = Cluster(context.system)
  val interval = 2
  var neighbors: Set[ActorRef] = Set.empty[ActorRef]
  var node: Option[ActorRef] = None
  var delay: Boolean = false
  val clock: Clock = Clock.systemDefaultZone
  var optimizeFor: String = "latency"
  var latencies: Map[ActorRef, scala.concurrent.duration.Duration] = Map.empty[ActorRef, scala.concurrent.duration.Duration]
  var simulatedCosts: Map[ActorRef, (ContinuousBoundedValue[Duration], ContinuousBoundedValue[Double])] =
    Map.empty[ActorRef, (ContinuousBoundedValue[Duration], ContinuousBoundedValue[Double])]
  var hostProps: HostProps = new HostProps(simulatedCosts)
  var hostToNodeMap: Map[ActorRef, ActorRef] = Map.empty[ActorRef, ActorRef]


  case class HostProps(costs : Map[ActorRef, (ContinuousBoundedValue[Duration], ContinuousBoundedValue[Double])]) {
    def advance = HostProps(
      costs map { case (host, (latency, bandwidth)) => (host, (latency.advance, bandwidth.advance)) })
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
      () => (18 - 30 * random.nextDouble, 1 + random.nextInt(10)))

    def apply() =
      template copy (value = 5 + 95* random.nextDouble)
  }


  //
  //GREEDY STRATEGY

  val degree: Int = 2
  val random: scala.util.Random = scala.util.Random
  val system: ActorSystem = context.system

  var activeOperator: Option[ActiveOperator] = None
  var tentativeOperator: Option[TentativeOperator] = None

  var tentativeHosts: Seq[ActorRef] = Seq.empty[ActorRef]

  var childHost1: Option[ActorRef] = None
  var childHost2: Option[ActorRef] = None
  var childNode1: Option[ActorRef] = None
  var childNode2: Option[ActorRef] = None
  var optimumChildHost1: Option[ActorRef] = None
  var optimumChildHost2: Option[ActorRef] = None

  var finishedChildren: Int = 0
  var completedChildren: Int = 0
  var processedCostMessages: Int = 0
  var children: Map[ActorRef, Seq[ActorRef]] = Map.empty[ActorRef, Seq[ActorRef]]
  var childCosts: Map[ActorRef, (Duration, Double)] = Map.empty[ActorRef, (Duration, Double)]
  var parent: Option[ActorRef] = None
  var consumer: Boolean = false
  var ready: Boolean = false

  var optimumHosts: Seq[ActorRef] = Seq.empty[ActorRef]

  var receivedResponses: Set[ActorRef] = Set.empty

  var parentNode: Option[ActorRef] = None
  var parentHosts: Seq[ActorRef] = Seq.empty[ActorRef]

  var costs: Map[ActorRef, (Duration, Double)] = Map.empty

  var operators: Map[ActorRef, Option[Operator]] = Map.empty[ActorRef, Option[Operator]]

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
    neighbors.foreach(neighbor => simulatedCosts += neighbor -> (latency(), bandwidth()))
    hostProps = HostProps(simulatedCosts)
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

  def delay(delay: Boolean): Unit = {
    if(node.isDefined) {
      node.get ! Delay(delay)
    }
    this.delay = delay
  }

  def reportCostsToNode(): Unit = {
    var result = Map.empty[ActorRef, Cost]
    val map = hostPropsToMap
    hostToNodeMap.foreach(host => if(map.contains(host._1)){result += host._2 -> map(host._1)})
    if(node.isDefined){
      node.get ! HostPropsResponse(result)
    }
  }

  def hostPropsToMap: Map[ActorRef, Cost] = {
    hostProps.costs map { case (host, (latency, bandwidth)) => (host, Cost(latency.value, bandwidth.value)) }
  }

  def receive = {
    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
      //context.system.actorSelection(member.address.toString + "/user/Host") ! LatencyRequest(clock.instant())
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)
    case LatencyRequest(time) =>
      if(sender() != self){
        if(delay) {
          sender() ! LatencyResponse(time.minusMillis(40))
        } else {
          sender() ! LatencyResponse(time)
        }
        //otherHosts += sender()
        //println(otherHosts)
      }
    case LatencyResponse(requestTime) =>
      if(sender() != self) {
        latencies += sender() -> FiniteDuration(java.time.Duration.between(requestTime, clock.instant).dividedBy(2).toMillis, TimeUnit.MILLISECONDS)
        //neighbors += sender()
      }
    case Neighbors(n, h)=>
      neighbors = n
      h.foreach(host => simulatedCosts += host -> (latency(), bandwidth()))
      hostProps = HostProps(simulatedCosts)
    case AllHosts => {
      context.system.actorSelection(self.path.address.toString + "/user/Placement") ! Hosts(neighbors)
      //println("sending Hosts", sender(), Hosts(neighbors + self))
    }
    case Delay(b) =>{
      if(sender() != self){
        delay(b)
        //println("delaying")
      }
    }
    case Node(actorRef) =>{
      node = Some(actorRef)
      reportCostsToNode()
    }
    case OptimizeFor(o) => optimizeFor = o
    case gPE: GreedyPlacementEvent => processEvent(gPE, sender())
    case HostPropsRequest => sender() ! HostPropsResponse(hostPropsToMap)
    case _ =>
  }

  sealed trait Optimizing
  case object Maximizing extends Optimizing
  case object Minimizing extends Optimizing
  /*
    def startLatencyMonitoring(): Unit = context.system.scheduler.schedule(
      initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
      interval = FiniteDuration(5, TimeUnit.SECONDS),
      runnable = () => {
        parentHosts.foreach{ _ ! CostRequest(clock.instant)}
      })

    startLatencyMonitoring()
  */
  def chooseTentativeOperators() : Unit = {
    println("CHOOSING TENTATIVE OPERATORS")
    if (children.nonEmpty || parent.isDefined){
      if(activeOperator.isDefined){
        val neighborSeq: Seq[ActorRef] = neighbors.toSeq
        var timeout = 0
        var chosen: Boolean = false
        while (tentativeHosts.size < degree && timeout < 1000 && !chosen){
          val randomNeighbor =  neighborSeq(random.nextInt(neighborSeq.size))
          if(operators(randomNeighbor).isEmpty && !tentativeHosts.contains(randomNeighbor)){
            val tenOp = TentativeOperator(NodeHost(randomNeighbor), activeOperator.get.props, activeOperator.get.dependencies)
            randomNeighbor ! BecomeTentativeOperator(tenOp, parentNode.get, parentHosts, childHost1, childHost2)
            chosen = true
          }
          timeout += 1
        }
        if(timeout >= 1000){
          println("Not enough hosts available as tentative Operators. Continuing without...")
          children.toSeq.head._1 ! ChooseTentativeOperators(tentativeHosts :+ self)
        }
        //children.toSeq.head._1 ! ChooseTentativeOperators(tentativeHosts :+ self)
      } else {
        println("ERROR: Only Active Operator can choose Tentative Operators")
      }
    }
    else {
      parentHosts.foreach(_ ! FinishedChoosing(tentativeHosts))
    }
  }

  def setup() : Unit = {
    println("setting UP", neighbors)
    neighbors.foreach(neighbor => neighbor ! OperatorRequest)
  }

  def processEvent(event: GreedyPlacementEvent, sender: ActorRef): Unit ={
    event match {
      case m: CostMessage => processCostMessage(m, sender)
      case BecomeActiveOperator(operator) =>
        becomeActiveOperator(operator)
      case SetActiveOperator(operator) =>
        setActiveOperator(operator)
      case BecomeTentativeOperator(operator, p, pHosts, c1, c2) =>
        println("I'VE BEEN CHOSEN!!!", sender)
        parentNode = Some(p)
        parentHosts = pHosts
        childHost1 = c1
        childHost2 = c2
        becomeTentativeOperator(operator, sender)
      case TentativeAcknowledgement =>
        tentativeHosts = tentativeHosts :+ sender
        if(tentativeHosts.size == degree){
          children.toSeq.head._1 ! ChooseTentativeOperators(tentativeHosts :+ self)
        } else {
          chooseTentativeOperators()
        }
      case ContinueSearching =>
        setup()
      case ChooseTentativeOperators(parents) =>
        println("Choosing Tentative Operators")
        parentHosts = parents
        if(parents.isEmpty){
          consumer = true
          println(children)
          children.toSeq.head._1 ! ChooseTentativeOperators(Seq(self))
        } else if(children.isEmpty){
          parentHosts.foreach(_ ! FinishedChoosing(Seq.empty[ActorRef]))
        } else {
          setup()
        }
      case OperatorRequest =>
        println("Got Operator Request, Sending Response", isOperator, " to", sender)
        sender ! OperatorResponse(activeOperator, tentativeOperator)
      case OperatorResponse(ao, to) =>
        receivedResponses += sender
        println("Received Operator Response:")
        println("no. of responses ", receivedResponses.size)
        println("no. of neighbors ", neighbors.size)
        if(ao.isDefined){
          operators += sender -> ao
        } else {
          operators += sender -> to
        }
        if(receivedResponses.size == neighbors.size){
          chooseTentativeOperators()
        }
      case ParentResponse(p) =>
        parent = p
      case ChildResponse(c) =>
        println("Got Child Response", c)
        println(childHost1)
        println(childHost2)
        if(sender.equals(childHost1.get)){
          childNode1 = Some(c)
        } else if (childHost2.isDefined && sender.equals(childHost2.get)){
          childNode2 = Some(c)
        } else {
          println("ERROR: Got Child Response from non child")
        }
        if(childNodes.size == children.size){
          childNodes.size match {
            case 1 => node.get ! Child1(childNode1.get)
            case 2 => node.get ! Child2(childNode1.get, childNode2.get)
            case _ => println("ERROR: Got a Child Response but Node has no children")
          }
          childNode1 = None
          childNode2 = None
        }
      case ChildHost1(h) =>
        children += h -> Seq.empty[ActorRef]
        childHost1 = Some(h)
        h ! ParentHost(self, node.get)
      case ChildHost2(h1, h2) =>
        children += h1 -> Seq.empty[ActorRef]
        children += h2 -> Seq.empty[ActorRef]
        childHost1 = Some(h1)
        childHost2 = Some(h2)
        h1 ! ParentHost(self, node.get)
        h2 ! ParentHost(self, node.get)
      case ParentHost(p, ref) =>
        println("Got Parent",p ,ref)
        hostToNodeMap += p -> ref
        if(node.isDefined){
          reportCostsToNode()
          node.get ! Parent(ref)
          parentNode = Some(ref)
          parent = Some(p)
        }
      case FinishedChoosing(tChildren) =>
        children += sender -> tChildren
        finishedChildren += 1
        println("A child finished choosing tentative operators", finishedChildren, "/", children.size)
        if(activeOperator.isDefined) {
          if (finishedChildren == children.size) {
            if (consumer) {
              ready = true
              println("READY TO CALCULATE NEW PLACEMENT!")
            } else {
              parentHosts.foreach(_ ! FinishedChoosing(tentativeHosts))
            }
          } else if (finishedChildren < children.size) {
            children.toSeq(finishedChildren)._1 ! ChooseTentativeOperators(tentativeHosts :+ self)
          }
        }
      case Start =>
        parentHosts.foreach(p => p ! CostRequest(clock.instant()))
        if (activeOperator.isDefined) {
          broadcastMessage(Start)
        }
      case CostRequest(t) =>
        system.scheduler.scheduleOnce(
          FiniteDuration(hostPropsToMap(sender).duration.toMillis * 2, TimeUnit.MILLISECONDS),
          () => {sender ! CostResponse(t, hostPropsToMap(sender).bandwidth)})
      case CostResponse(t, _) =>
        if (parentHosts.contains(sender)) {
          costs += sender -> (FiniteDuration(java.time.Duration.between(t, clock.instant).dividedBy(2).toMillis, TimeUnit.MILLISECONDS), hostPropsToMap(sender).bandwidth)
          sendOutCostMessages()
        }
      case StateTransferMessage(o, p) =>
        hostToNodeMap += sender -> p
        parent = Some(sender)
        parentNode = Some(p)
        processStateTransferMessage(o)
      case RequirementsNotMet =>
        println(sender)
        if(consumer && ready){
          ready = false
          println("RECALCULATING")
          broadcastMessage(Start)
        }
      case RequirementsMet =>
      case MigrationComplete =>
        println("A Child completed Migration", sender)
        completedChildren += 1
        if(parent.isDefined){
          if (completedChildren == children.size) {
            parent.get ! MigrationComplete
          }
        } else if(consumer){
           if (completedChildren == children.size) {
              updateChildren()
              resetAllData(false)
              children.toSeq.head._1 ! ChooseTentativeOperators(tentativeHosts :+ self)
           }
        } else {
          println("ERROR: Something went terribly wrong")
        }
    }
  }

  def childNodes : Seq[ActorRef] = {
    if(childNode1.isDefined && childNode2.isDefined){
      Seq(childNode1.get, childNode2.get)
    } else if(childNode1.isDefined){
      Seq(childNode1.get)
    } else if(childNode2.isDefined){
      Seq(childNode2.get)
    }else {
      Seq.empty[ActorRef]
    }
  }

  def broadcastMessage(message: GreedyPlacementEvent): Unit ={
    children.foreach(child => {
      child._1 ! message
      child._2.foreach(tentativeChild => tentativeChild ! message)
    })
  }

  def processCostMessage(m: CostMessage, sender: ActorRef): Unit = {
    if(isOperator && isChild(sender)){
      childCosts += sender -> (m.latency, m.bandwidth)
      println(m.latency, m.bandwidth)
    }
    else {
      println("ERROR: Cost Message arrived at Host without Operator")
    }
    processedCostMessages += 1
    sendOutCostMessages()
  }

  private def isChild(actorRef: ActorRef): Boolean ={
    var isChild = false
    children.foreach(child =>
      if(child._1.equals(actorRef) || child._2.contains(actorRef)){
      isChild = true
    })
    isChild
  }

  private def sendOutCostMessages() : Unit = {
    if(children.isEmpty && costs.size == parentHosts.size){
      parentHosts.foreach(parent => parent ! CostMessage(costs(parent)._1, costs(parent)._2))
    }
    else if (processedCostMessages == numberOfChildren && costs.size == parentHosts.size) {
      calculateOptimumNodes()
      println(optimumHosts)
      var bottleNeckNode = self
      if(optimizeFor == "latency"){
        bottleNeckNode = minmaxBy(Maximizing, optimumHosts)(childCosts(_)._1)
      }else if(optimizeFor == "bandwidth"){
        bottleNeckNode = minmaxBy(Minimizing, optimumHosts)(childCosts(_)._2)
      }else{
        bottleNeckNode = minmaxBy(Minimizing, optimumHosts)(childCosts(_))
      }


      println(bottleNeckNode)
      childHost1 = optimumChildHost1
      childHost2 = optimumChildHost2
      optimumChildHost1 = None
      optimumChildHost2 = None
      //minmaxBy(Minimizing, costs)(_._2._1)._1
      parentHosts.foreach(parent => parent ! CostMessage(mergeLatency(childCosts(bottleNeckNode)._1, costs(parent)._1),
        mergeBandwidth(childCosts(bottleNeckNode)._2, costs(parent)._2)))
      if (consumer) {
        broadcastMessage(StateTransferMessage(optimumHosts, node.get))
      }
    }
    println(children.isEmpty, processedCostMessages, numberOfChildren, costs.size, parentHosts.size)
  }

  def mergeBandwidth(b1: Double, b2: Double): Double = {
    Math.min(b1,b2)
  }

  implicit val ordering: Ordering[(Duration, Double)] = new Ordering[(Duration, Double)] {
    def abs(x: Duration) = if (x < Duration.Zero) -x else x

    def compare(x: (Duration, Double), y: (Duration, Double)) = ((-x._1, x._2), (-y._1, y._2)) match {
      case ((d0, n0), (d1, n1)) if d0 == d1 && n0 == n1 => 0
      case ((d0, n0), (d1, n1)) if d0 < d1 && n0 < n1 => -1
      case ((d0, n0), (d1, n1)) if d0 > d1 && n0 > n1 => 1
      case ((d0, n0), (d1, n1)) =>
        math.signum((d0 - d1) / abs(d0 + d1) + (n0 - n1) / math.abs(n0 + n1)).toInt
    }
  }

  def calculateOptimumNodes() : Unit = {
    println(childHost1)
    println(childHost2)

    if(optimizeFor == "latency"){
      children.toSeq.foreach(child => optimumHosts = optimumHosts :+ minmaxBy(Minimizing,
        getChildAndTentatives(child._1))(childCosts(_)._1))
    }else if(optimizeFor == "bandwidth"){
      children.toSeq.foreach(child => optimumHosts = optimumHosts :+ minmaxBy(Maximizing,
        getChildAndTentatives(child._1))(childCosts(_)._2))
    }else{
      children.toSeq.foreach(child => optimumHosts = optimumHosts :+ minmaxBy(Maximizing,
        getChildAndTentatives(child._1))(childCosts(_)))
    }

    optimumHosts.foreach(host =>
      if(childHost1.isDefined && getPreviousChild(host) == childHost1.get){
        optimumChildHost1 = Some(host)
        println(optimumChildHost1)
        println(getPreviousChild(host))
      } else if(childHost2.isDefined && getPreviousChild(host) == childHost2.get){
        optimumChildHost2 = Some(host)
        println(optimumChildHost2)
        println(getPreviousChild(host))
      } else {
        println("ERROR: optimumHost does not belong to a child")
      }
    )
  }

  def getPreviousChild(actorRef: ActorRef): ActorRef = {
    children.foreach(child => if(child._1.equals(actorRef) || child._2.contains(actorRef)) return child._1)
    null
  }

  def getChildAndTentatives(actorRef: ActorRef): Seq[ActorRef] = {
    Seq(actorRef) ++ children(actorRef)
  }

  def setNode(actorRef: ActorRef): Unit = {
    node = Some(actorRef)
    if(parentNode.isDefined){
      node.get ! Parent(parentNode.get)
    }
  }

  def mergeLatency(latency1: Duration, latency2: Duration): Duration ={
    latency1.+(latency2)
  }

  def processStateTransferMessage(oHosts: Seq[ActorRef]): Unit ={
    println("PROCESSING STATE TRANSFER....")
    if(oHosts.contains(self)){
      if(activeOperator.isDefined){
        reportCostsToNode()
        node.get ! Parent(parentNode.get)
        if(children.nonEmpty){
          broadcastMessage(StateTransferMessage(optimumHosts, node.get))
          updateChildren()
        } else {
          parent.get ! MigrationComplete
        }
      }
      if(tentativeOperator.isDefined){
        activate()
        reportCostsToNode()
        node.get ! Parent(parentNode.get)
        if(children.nonEmpty) {
          broadcastMessage(StateTransferMessage(optimumHosts, node.get))
          updateChildren()
        } else {
          parent.get ! MigrationComplete
        }
      }
      resetAllData(false)
      if(parent.isDefined && node.isDefined){
        parent.get ! ChildResponse(node.get)
      }
    } else {
      println("DEACTIVATING....")
      resetAllData(true)
    }
  }

  def updateChildren(): Unit = {
    children = Map.empty[ActorRef, Seq[ActorRef]]
    optimumHosts.foreach(host => children += host -> Seq.empty[ActorRef])
  }

  def resetAllData(deleteEverything: Boolean): Unit ={
    if(deleteEverything){
      if(node.isDefined){
        node.get ! Kill
      }
      node = None
      parent = None
      activeOperator = None
      tentativeOperator = None
      children = Map.empty[ActorRef, Seq[ActorRef]]
      childHost1 = None
      childHost2 = None
    }

    childCosts = Map.empty[ActorRef, (Duration, Double)]

    parentHosts = Seq.empty[ActorRef]
    costs = Map.empty[ActorRef, (Duration, Double)]

    optimumHosts = Seq.empty[ActorRef]
    tentativeHosts = Seq.empty[ActorRef]

    finishedChildren = 0
    completedChildren = 0
    processedCostMessages = 0
    receivedResponses = Set.empty[ActorRef]

    ready = false

    operators = Map.empty[ActorRef, Option[Operator]]
  }

  def setOperators(sender: ActorRef, activeOperator: Option[ActiveOperator], tentativeOperator: Option[TentativeOperator]): Unit = {
    if (activeOperator.isDefined){
      operators += sender -> activeOperator
    }
    else
      operators += sender -> tentativeOperator
  }

  def setActiveOperator(props: Props): Unit ={
    if(!isOperator){
      activeOperator = Some(ActiveOperator(null, props, null))
    }else{
      println("ERROR: Host already has an Operator")
    }
  }

  def becomeActiveOperator(operator: ActiveOperator): Unit ={
    if(!isOperator){
      activeOperator = Some(operator)
      val temp = system.actorOf(activeOperator.get.props.withDeploy(Deploy(scope = remote.RemoteScope(self.path.address))))
      println(temp)
      node = Some(temp)
      node.get ! Controller(self)
      node.get ! Delay(delay)

    }else{
      println("ERROR: Host already has an Operator")
    }
  }

  def becomeTentativeOperator(operator: TentativeOperator, sender: ActorRef): Unit ={
    if(!isOperator){
      tentativeOperator = Some(operator)
      sender ! TentativeAcknowledgement
    } else {
      println("ERROR: Host already has a Operator")
      sender ! ContinueSearching
    }
  }

  def activate() : Unit = {
    println("ACTIVATING...")
    if(tentativeOperator.isDefined){
      activeOperator = Some(ActiveOperator(tentativeOperator.get.host, tentativeOperator.get.props, tentativeOperator.get.dependencies))
      val temp = system.actorOf(activeOperator.get.props.withDeploy(Deploy(scope = remote.RemoteScope(self.path.address))))
      println(temp)
      node = Some(temp)
      node.get ! Controller(self)
      tentativeOperator = None
    } else {
      println("ERROR: Cannot Activate without tentative Operator present")
    }
  }

  def numberOfChildren: Int ={
    var count = 0
    children.foreach(child => count += (child._2.size + 1))
    count
  }

  def isOperator: Boolean ={
    activeOperator.isDefined || tentativeOperator.isDefined
  }

  def getOperator: Option[Operator] = {
    if(isOperator){
      if(activeOperator.isDefined){
        activeOperator
      }
      else tentativeOperator
    }
    else None
  }

  private def minmax[T: Ordering](optimizing: Optimizing, traversable: TraversableOnce[T]): T = optimizing match {
    case Maximizing => traversable.min
    case Minimizing => traversable.max
  }

  private def minmaxBy[T, U: Ordering](optimizing: Optimizing, traversable: TraversableOnce[T])(f: T => U): T = optimizing match {
    case Maximizing => traversable maxBy f
    case Minimizing => traversable minBy f
  }

}