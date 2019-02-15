package adaptivecep.distributed

import java.util.concurrent.TimeUnit

import adaptivecep.data.Cost.Cost
import adaptivecep.data.Events._
import adaptivecep.data.Queries.Requirement
import adaptivecep.distributed.operator._
import adaptivecep.distributed.operator.{ActiveOperator, Host, HostProps, NodeHost, Operator, TentativeOperator, helper}
import akka.actor.{ActorRef, ActorSystem, Deploy, Props}
import akka.cluster.ClusterEvent._
import akka.remote
import rescala.default._
import rescala.{default, _}
import rescala.core.{CreationTicket, ReSerializable}
import rescala.default.{Evt, Signal, Var}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

trait HostActorDecentralizedBase extends HostActorBase with System{

  sealed trait Optimizing
  case object Maximizing extends Optimizing
  case object Minimizing extends Optimizing

  val degree: Int = 2
  val system: ActorSystem = context.system


  var activeOperator: Option[ActiveOperator] = None
  var tentativeOperator: Option[TentativeOperator] = None

  var tentativeHosts: Set[NodeHost] = Set.empty[NodeHost]

  var childHost1: Option[ActorRef] = None
  var childHost2: Option[ActorRef] = None
  var childNode1: Option[ActorRef] = None
  var childNode2: Option[ActorRef] = None
  var optimumChildHost1: Option[ActorRef] = None
  var optimumChildHost2: Option[ActorRef] = None

  var finishedChildren: Int = 0
  var completedChildren: Int = 0
  var processedCostMessages: Int = 0
  var children: Map[NodeHost, Set[NodeHost]] = Map.empty[NodeHost, Set[NodeHost]]
  var childCosts: Map[NodeHost, (Duration, Double)] = Map.empty[NodeHost, (Duration, Double)]
  var parent: Option[NodeHost] = None
  var consumer: Boolean = false


  var optimumHosts: Set[NodeHost] = Set.empty[NodeHost]

  var receivedResponses: Var[Set[ActorRef]] = Var(Set.empty[ActorRef])(ReSerializable.doNotSerialize, "consumers")
  var latencyResponses: Var[Set[ActorRef]] = Var(Set.empty[ActorRef])(ReSerializable.doNotSerialize, "consumers")
  var bandwidthResponses: Var[Set[ActorRef]] = Var(Set.empty[ActorRef])(ReSerializable.doNotSerialize, "consumers")

  var ready: Signal[Boolean] = Signal{(finishedChildren == children.size) && consumer}

  var parentNode: Option[ActorRef] = None
  var parentHosts: Set[NodeHost] = Set.empty[NodeHost]

  //var operators: Map[ActorRef, Option[Operator]] = Map.empty[ActorRef, Option[Operator]]

  val thisHost: Signal[NodeHost] = Signal{NodeHost(self)}

  val placement: Var[Map[Operator, Host]] = Var(Map.empty[Operator, Host])
  val reversePlacement: Signal[Map[Host, Operator]] = Signal{placement().map(_.swap)}


  val costSignal: Var[Map[Host, Map[Host, Cost]]] = Var(Map.empty[Host, Map[Host, Cost]])(ReSerializable.doNotSerialize, "cost")
  val qos: Signal[Map[Host, HostProps]] = Signal{helper.hostProps(costSignal(),hosts().map(h => h.asInstanceOf[Host]))}
  val consumers: Var[Set[Operator]] = Var(Set.empty[Operator])(ReSerializable.doNotSerialize, "consumers")
  val producers: Var[Set[Operator]] = Var(Set.empty[Operator])(ReSerializable.doNotSerialize, "producers")
  //val operators: Var[Set[Operator]] = Var(Set.empty[Operator])(ReSerializable.doNotSerialize, "operators")
  val demandViolated: default.Evt[Set[Requirement]] = Evt[Set[Requirement]]()

  val migrationComplete: default.Evt[Unit] = Evt[Unit]()

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

  def resetAllData(bool: Boolean): Unit
  def adapt(): Unit

  override def preStart(): Unit = {
    tick += {_ => measureCosts(hosts.now)}
    migrationComplete += {_ => completeMigration}
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
    startSimulation()
  }

  override def startSimulation(): Unit = context.system.scheduler.schedule(
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
      //measureCosts()
      reportCostsToNode()
    })

  override def receive = {
    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
    //context.system.actorSelection(member.address.toString + "/user/Host") ! LatencyRequest(clock.instant())
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)
    case Hosts(h)=>
      hosts.set(h.map(host => NodeHost(host)))
      hosts.now.foreach(host => simulatedCosts += host -> (latency(), bandwidth()))
      hostProps = HostPropsSimulator(simulatedCosts)
    case Node(actorRef) =>{
      node = Some(actorRef)
      reportCostsToNode()
    }
    case OptimizeFor(o) => optimizeFor = o
    case LatencyRequest(t)=>
      sender() ! LatencyResponse(t)
    case LatencyResponse(t) =>
      latencyResponses.set(latencyResponses.now + sender())
      val latency = FiniteDuration(java.time.Duration.between(t, clock.instant()).dividedBy(2).toMillis, TimeUnit.MILLISECONDS)
      costs += hostMap(sender()) -> Cost(latency, costs(hostMap(sender())).bandwidth)
      //sendOutCostMessages()
    case StartThroughPutMeasurement(instant) =>
      throughputStartMap += hostMap(sender()) -> (instant, clock.instant())
      throughputMeasureMap += hostMap(sender()) -> 0
    case TestEvent =>
      throughputMeasureMap += hostMap(sender()) -> (throughputMeasureMap(hostMap(sender())) + 1)
    case EndThroughPutMeasurement(instant, actual) =>
      val senderDiff = java.time.Duration.between(throughputStartMap(hostMap(sender()))._1, instant)
      val receiverDiff = java.time.Duration.between(throughputStartMap(hostMap(sender()))._2, clock.instant())
      val bandwidth = (senderDiff.toMillis.toDouble / receiverDiff.toMillis.toDouble) * ((1000 / senderDiff.toMillis) * 1000/*throughputMeasureMap(sender())*/)
      sender() ! ThroughPutResponse(bandwidth.toInt)
      //println(bandwidth, actual)
      throughputMeasureMap += hostMap(sender()) -> 0
    case ThroughPutResponse(r) =>
      costs += hostMap(sender()) -> Cost(costs(hostMap(sender())).duration, r)
      bandwidthResponses.set(bandwidthResponses.now + sender())
      //sendOutCostMessages()
    case gPE: PlacementEvent => processEvent(gPE, sender())
    case HostPropsRequest => send(hostMap(sender()), HostPropsResponse(hostPropsToMap))
    case _ =>
  }

  def processEvent(event: PlacementEvent, sender: ActorRef): Unit ={
    event match {
      case m: CostMessage => processCostMessage(m, sender)
      case BecomeActiveOperator(operator) => becomeActiveOperator(operator)
      case SetActiveOperator(operator) => setActiveOperator(operator)
      case BecomeTentativeOperator(operator, p, pHosts, c1, c2, _) => becomeTentativeOperator(operator, sender, p, pHosts, c1, c2)
      case TentativeAcknowledgement => processAcknowledgement(sender)

      case ContinueSearching => setup(hosts.now)

      case ChooseTentativeOperators(parents) => setUpTentatives(parents)
      case OperatorRequest => send(hostMap(sender), OperatorResponse(activeOperator, tentativeOperator))
      case OperatorResponse(ao, to) => processOperatorResponse(sender, ao, to)
      case ParentResponse(p) => if(p.isDefined) parent = Some(hostMap(p.get)) else parent = None
      case ChildResponse(c) => processChildResponse(sender, c)
      case ChildHost1(h) => receiveChildHost(hostMap(h))
      case ChildHost2(h1, h2) => receiveChildHosts(h1, h2)
      case ParentHost(p, ref) => receiveParentHost(p, ref)
      case FinishedChoosing(tChildren) => childFinishedChoosingTentatives(sender, tChildren)
      //   case StateTransferMessage(o, p) => processStateTransferMessage(o, p)
      case RequirementsNotMet(requirements) => demandViolated.fire(requirements)
      case MigrationComplete => migrationComplete.fire()
      case _ =>
    }
  }


  def chooseTentativeOperators(hosts: Set[NodeHost],
                               children : Map[NodeHost, Set[NodeHost]],
                               parent: Option[NodeHost],
                               activeOperator: Option[ActiveOperator],
                               tentativeHosts: Set[NodeHost],
                               operators: Map[Host, Operator],
                               degree: Int,
                               thisHost: NodeHost,
                               parentHosts: Set[NodeHost]) : Unit = {
    //println("CHOOSING TENTATIVE OPERATORS")
    if (children.nonEmpty || parent.isDefined){
      if(activeOperator.isDefined){
        var timeout = 0
        var chosen: Boolean = false
        while (tentativeHosts.size < degree && timeout < 1000 && !chosen){
          val randomNeighbor =  hosts.toVector(random.nextInt(hosts.size))
          if(operators.contains(randomNeighbor) && !tentativeHosts.contains(randomNeighbor)){
            val tenOp = TentativeOperator(activeOperator.get.props, activeOperator.get.dependencies)
            send(randomNeighbor, BecomeTentativeOperator(tenOp, parentNode.get, parentHosts, childHost1, childHost2, 0))
            chosen = true
          }
          timeout += 1
        }
        if(timeout >= 1000){
          //println("Not enough hosts available as tentative Operators. Continuing without...")
          send(children.toSeq.head._1, ChooseTentativeOperators(tentativeHosts + thisHost))
        }
        //children.toSeq.head._1 ! ChooseTentativeOperators(tentativeHosts :+ self)
      } else {
        println("ERROR: Only Active Operator can choose Tentative Operators")
      }
    }
    else {
      parentHosts.foreach(send(_, FinishedChoosing(tentativeHosts)))
    }
  }

  def setup(hosts: Set[NodeHost]) : Unit = {
    //println("setting UP", neighbors)
    receivedResponses.set(Set.empty[ActorRef])
    hosts.foreach(neighbor => send(neighbor, OperatorRequest))
  }

  /*def adapt: Unit = {
    if (consumer && ready) {
      ready = false
      broadcastMessage(Start)
    }
  }*/

  def receiveChildHost(h: NodeHost): Unit = {
    children += h -> Set.empty[NodeHost]
    childHost1 = Some(h.actorRef)
    send(h, ParentHost(self, node.get))
  }

  def processOperatorResponse(sender: ActorRef, ao: Option[ActiveOperator], to: Option[TentativeOperator]): Unit = {
    receivedResponses.set(receivedResponses.now + sender)
    if (ao.isDefined) {
      placement.set(placement.now + (ao.get -> hostMap(sender)))
    } else {
      placement.set(placement.now + (to.get -> hostMap(sender)))
    }
    if (receivedResponses.now.size == hosts.now.size) {
      chooseTentativeOperators(hosts.now, children, parent, activeOperator, tentativeHosts, reversePlacement.now, degree, thisHost.now, parentHosts)
    }
  }

  def receiveParentHost(p: ActorRef, ref: ActorRef): Unit = {
    hostToNodeMap += hostMap(p) -> ref
    if (node.isDefined) {
      reportCostsToNode()
      node.get ! Parent(ref)
      parentNode = Some(ref)
      parent = Some(hostMap(p))
    }
  }

  def receiveChildHosts(h1: ActorRef, h2: ActorRef): Unit = {
    children += hostMap(h1) -> Set.empty[NodeHost]
    children += hostMap(h2) -> Set.empty[NodeHost]
    childHost1 = Some(h1)
    childHost2 = Some(h2)
    send(hostMap(h1), ParentHost(self, node.get))
    send(hostMap(h2), ParentHost(self, node.get))
  }

  def completeMigration = {
    completedChildren += 1
    if (parent.isDefined) {
      if (completedChildren == children.size) {
        send(parent.get, MigrationComplete)
      }
    } else if (consumer) {
      if (completedChildren == children.size) {
        updateChildren()
        resetAllData(false)
        send(children.toSeq.head._1, ChooseTentativeOperators(tentativeHosts + thisHost.now))
      }
    } else {
      println("ERROR: Something went terribly wrong")
    }
  }

  def setUpTentatives(parents: Set[NodeHost]) = {
    parentHosts = parents
    if (parents.isEmpty) {
      consumer = true
      send(children.toSeq.head._1, ChooseTentativeOperators(Set(thisHost.now)))
    } else if (children.isEmpty) {
      parentHosts.foreach(send(_, FinishedChoosing(Set.empty[NodeHost])))
    } else {
      setup(hosts.now)
    }
  }

  def processAcknowledgement(sender: ActorRef) = {
    tentativeHosts = tentativeHosts + hostMap(sender)
    if (tentativeHosts.size == degree) {
      send(children.toSeq.head._1, ChooseTentativeOperators(tentativeHosts + thisHost.now))
    } else {
      chooseTentativeOperators(hosts.now, children, parent, activeOperator, tentativeHosts, reversePlacement.now, degree, thisHost.now, parentHosts)
    }
  }

  def processChildResponse(sender: ActorRef, c: ActorRef) = {
    if (childHost1.isDefined && sender.equals(childHost1.get)) {
      childNode1 = Some(c)
    } else if (childHost2.isDefined && sender.equals(childHost2.get)) {
      childNode2 = Some(c)
    } else {
      println("ERROR: Got Child Response from non child")
    }
    if (childNodes.size == children.size) {
      childNodes.size match {
        case 1 => node.get ! Child1(childNode1.get)
        case 2 => node.get ! Child2(childNode1.get, childNode2.get)
        case _ => println("ERROR: Got a Child Response but Node has no children")
      }
      childNode1 = None
      childNode2 = None
    }
  }

  def childFinishedChoosingTentatives(sender: ActorRef, tChildren: Set[NodeHost]) = {
    children += hostMap(sender) -> tChildren
    finishedChildren += 1
    //println("A child finished choosing tentative operators", finishedChildren, "/", children.size)
    if (activeOperator.isDefined) {
      if (finishedChildren == children.size) {
        if (consumer) {
          //ready = true
          //println("READY TO CALCULATE NEW PLACEMENT!")
        } else {
          parentHosts.foreach(send(_, FinishedChoosing(tentativeHosts)))
        }
      } else if (finishedChildren < children.size) {
        children.toSeq(finishedChildren)._1.actorRef ! ChooseTentativeOperators(tentativeHosts + thisHost.now)
      }
    }
  }

  override def measureCosts(hosts: Set[NodeHost]):Unit = {
    val now = clock.instant()
    for (p <- parentHosts) {
      if (hostPropsToMap.contains(p)) {
        p.actorRef ! StartThroughPutMeasurement(now)
        context.system.scheduler.scheduleOnce(
          FiniteDuration((bandwidth.template.max / hostPropsToMap(p).bandwidth).toLong * 100, TimeUnit.MILLISECONDS),
          () => {
            p.actorRef ! EndThroughPutMeasurement(now.plusMillis(100), hostPropsToMap(p).bandwidth.toInt)
          })
        if (hostPropsToMap.contains(p)) {
          context.system.scheduler.scheduleOnce(
            FiniteDuration(hostPropsToMap(p).duration.toMillis * 2, TimeUnit.MILLISECONDS),
            () => {
              p.actorRef ! LatencyRequest(now)
            })
        } else {
          p.actorRef ! LatencyRequest(now)
        }
      }
    }
    /*if (activeOperator.isDefined) {
      broadcastMessage(Start)
    }*/
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

  def broadcastMessage(message: PlacementEvent): Unit ={
    children.foreach(child => {
      send(child._1, message)
      child._2.foreach(tentativeChild => send(tentativeChild, message))
    })
  }

  def processCostMessage(m: CostMessage, sender: ActorRef): Unit = {
    if(isOperator && isChild(sender)){
      childCosts += hostMap(sender) -> (m.latency, m.bandwidth)
      //println(m.latency, m.bandwidth)
    }
    else {
      println("ERROR: Cost Message arrived at Host without Operator")
    }
    processedCostMessages += 1
    //sendOutCostMessages()
  }

  private def isChild(actorRef: ActorRef): Boolean ={
    var isChild = false
    children.foreach(child =>
      if(child._1.equals(hostMap(actorRef)) || child._2.contains(hostMap(actorRef))){
        isChild = true
      })
    isChild
  }

  def mergeBandwidth(b1: Double, b2: Double): Double = {
    Math.min(b1,b2)
  }

  def getPreviousChild(actorRef: ActorRef): ActorRef = {
    children.foreach(child => if(child._1.equals(hostMap(actorRef)) || child._2.contains(hostMap(actorRef))) return child._1.actorRef)
    null
  }

  def getChildAndTentatives(actorRef: ActorRef): Set[NodeHost] = {
    Set(hostMap(actorRef)) ++ children(hostMap(actorRef))
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

  def processStateTransferMessage(oHosts: Seq[ActorRef], p: ActorRef): Unit ={
    //println("PROCESSING STATE TRANSFER....")
    hostToNodeMap += hostMap(sender) -> p
    parent = Some(hostMap(sender))
    parentNode = Some(p)
    if(oHosts.contains(self)){
      if(activeOperator.isDefined){
        reportCostsToNode()
        node.get ! Parent(parentNode.get)
        if(children.nonEmpty){
          broadcastMessage(StateTransferMessage(optimumHosts, node.get))
          updateChildren()
        } else {
          send(parent.get, MigrationComplete)
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
          send(parent.get, MigrationComplete)
        }
      }
      resetAllData(false)
      if(parent.isDefined && node.isDefined){
        send(parent.get, ChildResponse(node.get))
      }
    } else {
      //println("DEACTIVATING....")
      resetAllData(true)
    }
  }

  def updateChildren(): Unit = {
    children = Map.empty[NodeHost, Set[NodeHost]]
    optimumHosts.foreach(host => children += host -> Set.empty[NodeHost])
  }

  /*def setOperators(sender: ActorRef, activeOperator: Option[ActiveOperator], tentativeOperator: Option[TentativeOperator]): Unit = {
    if (activeOperator.isDefined){
      operators += sender -> activeOperator
    }
    else
      operators += sender -> tentativeOperator
  }*/

  def setActiveOperator(props: Props): Unit ={
    if(!isOperator){
      activeOperator = Some(ActiveOperator(props, null))
    }else{
      println("ERROR: Host already has an Operator")
    }
  }

  def becomeActiveOperator(operator: ActiveOperator): Unit ={
    if(!isOperator){
      activeOperator = Some(operator)
      val temp = system.actorOf(activeOperator.get.props.withDeploy(Deploy(scope = remote.RemoteScope(self.path.address))))
      node = Some(temp)
      node.get ! Controller(self)

    }else{
      println("ERROR: Host already has an Operator")
    }
  }

  def becomeTentativeOperator(operator: TentativeOperator, sender: ActorRef, p: ActorRef, pHosts: Set[NodeHost], c1 : Option[ActorRef], c2: Option[ActorRef]): Unit ={
    parentNode = Some(p)
    parentHosts = pHosts
    childHost1 = c1
    childHost2 = c2
    if(!isOperator){
      tentativeOperator = Some(operator)
      send(hostMap(sender), TentativeAcknowledgement)
    } else {
      //println("ERROR: Host already has an Operator")
      send(hostMap(sender), ContinueSearching)
    }
  }

  def activate() : Unit = {
    //println("ACTIVATING...")
    if(tentativeOperator.isDefined){
      activeOperator = Some(ActiveOperator(tentativeOperator.get.props, tentativeOperator.get.dependencies))
      val temp = system.actorOf(activeOperator.get.props.withDeploy(Deploy(scope = remote.RemoteScope(self.path.address))))
      //println(temp)
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

  def minmax[T: Ordering](optimizing: Optimizing, traversable: TraversableOnce[T]): T = optimizing match {
    case Maximizing => traversable.min
    case Minimizing => traversable.max
  }

  def minmaxBy[T, U: Ordering](optimizing: Optimizing, traversable: TraversableOnce[T])(f: T => U): T = optimizing match {
    case Maximizing => traversable maxBy f
    case Minimizing => traversable minBy f
  }
}
