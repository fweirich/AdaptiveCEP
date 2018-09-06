package adaptivecep.distributed.strategy1

import java.time.Clock
import java.util.concurrent.TimeUnit

import adaptivecep.data.Events._
import adaptivecep.distributed._
import akka.actor.{ActorContext, ActorRef, ActorSystem, PoisonPill}

import scala.concurrent.duration.{Duration, FiniteDuration}

class GreedyPlacementCalculator (self: ActorRef, context: ActorContext, neighbors: Set[ActorRef]){

  val degree: Int = 2
  val random: scala.util.Random = scala.util.Random
  val clock: Clock = Clock.systemDefaultZone
  val system: ActorSystem = context.system

  var activeOperator: Option[ActiveOperator] = None
  var tentativeOperator: Option[TentativeOperator] = None

  var tentativeHosts: Seq[ActorRef] = Seq.empty[ActorRef]

  var finishedChildren: Int = 0
  var processedCostMessages: Int = 0
  var children: Map[ActorRef, Seq[ActorRef]] = Map.empty[ActorRef, Seq[ActorRef]]
  var childCosts: Map[ActorRef, Duration] = Map.empty[ActorRef, Duration]
  var childNodes: Seq[ActorRef] = Seq.empty[ActorRef]
  var parent: Option[ActorRef] = None
  var node: Option[ActorRef] = None
  var consumer: Boolean = false
  var ready: Boolean = false

  var optimumHosts: Seq[ActorRef] = Seq.empty[ActorRef]

  var receivedResponses: Set[ActorRef] = Set.empty

  var parentNode: Option[ActorRef] = None
  var parentHosts: Seq[ActorRef] = Seq.empty[ActorRef]

  var costs: Map[ActorRef, (Duration, Double)] = Map.empty

  var operators: Map[ActorRef, Option[Operator]] = Map.empty[ActorRef, Option[Operator]]

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
    if (children.nonEmpty){
      if(activeOperator.isDefined){
        val neighborSeq: Seq[ActorRef] = neighbors.toSeq
        var chosen: Set[ActorRef] = Set.empty
        var timeout = 0
        while (chosen.size < degree && timeout < 1000){
          var randomNeighbor =  neighborSeq(random.nextInt(neighborSeq.size))
          if(operators(randomNeighbor).isEmpty){
            chosen += randomNeighbor
            val tenOp = TentativeOperator(NodeHost(randomNeighbor), activeOperator.get.props, activeOperator.get.dependencies)
            tentativeHosts = tentativeHosts :+ randomNeighbor
            //randomNeighbor ! BecomeTentativeOperator(tenOp, parentNode.get, parentHosts)
          }
          timeout += 1
        }
        if(timeout >= 1000){
          println("Not enough hosts available as tentative Operators. Continuing without...")
        }
        children.toSeq.head._1 ! ChooseTentativeOperators(tentativeHosts)
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
      case BecomeTentativeOperator(operator, p, pHosts, c1, c2) =>
        parentNode = Some(p)
        parentHosts = pHosts
        becomeTentativeOperator(operator)
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
        println("Got Operator Request, Sending Response to", sender)
        sender ! OperatorResponse(activeOperator, tentativeOperator)
      case OperatorResponse(ao, to) =>
        println("Received Operator Response:")
        println("no. of responses ", receivedResponses)
        println("no. of neighbors ", neighbors.size)
        if(ao.isDefined){
          operators += sender -> ao
        } else {
          operators += sender -> to
        }
        receivedResponses += sender
        if(receivedResponses.size == neighbors.size){
          chooseTentativeOperators()
        }
      case ParentResponse(p) =>
        parent = p
      case ChildResponse(c) =>
        childNodes = childNodes :+ c
        if(childNodes.size == children.size){
          childNodes.size match {
            case 1 => node.get ! Child1(childNodes.head)
            case 2 => node.get ! Child2(childNodes.head, childNodes(1))
            case _ => println("ERROR Got a Child Response but Node has no children")
          }
        }
      case ChildHost1(h) =>
        children += h -> Seq.empty[ActorRef]
        h ! ParentHost(self, node.get)
      case ChildHost2(h1, h2) =>
        children += h1 -> Seq.empty[ActorRef]
        children += h2 -> Seq.empty[ActorRef]
        h1 ! ParentHost(self, node.get)
        h2 ! ParentHost(self, node.get)
      case ParentHost(p, ref) =>
        if(node.isDefined){
          node.get ! Parent(ref)
          parent = Some(p)
        }
      case FinishedChoosing(tChildren) =>
        println("A child finished choosing tentative operators", finishedChildren, "/", children.size)
        children += sender -> tChildren
        if(activeOperator.isDefined) {
          finishedChildren += 1
          if (finishedChildren == children.size) {
            if (consumer) {
              ready = true
            } else {
              parent.get ! FinishedChoosing(tentativeHosts)
            }
          } else {
            children.toSeq(finishedChildren)._1 ! ChooseTentativeOperators(tentativeHosts)
          }
        }
      case Start =>
        if(children.isEmpty){
          parentHosts.foreach(p => p ! CostRequest(clock.instant()))
        } else {
          broadcastMessage(Start)
        }
      case CostRequest(t) =>
        sender ! CostResponse(t)
      case CostResponse(t) =>
        if (parentHosts.contains(sender)) {
          costs += sender -> (FiniteDuration(java.time.Duration.between(t, clock.instant).dividedBy(2).toMillis, TimeUnit.MILLISECONDS), 0)
          sendOutCostMessages()
        }
      case StateTransferMessage(o, p) =>
        parent = Some(sender)
        parentNode = Some(p)
        processStateTransferMessage(o)
      case RequirementsNotMet =>
        if(consumer && ready){
          ready = false
          println("RECALCULATING")
          broadcastMessage(Start)
        }
      case MigrationComplete =>
        if(parent.isDefined){
          parent.get ! MigrationComplete
        } else if(consumer){
          setup()
        } else {
          println("ERROR: Something went terribly wrong")
        }
    }
  }

  def broadcastMessage(message: GreedyPlacementEvent): Unit ={
    children.foreach(child => {
      child._1 ! message
      child._2.foreach(tentativeChild => tentativeChild ! message)
    })
  }

  def processCostMessage(m: CostMessage, sender: ActorRef): Unit = {
    if(isOperator){
      if(children.contains(sender)){
        childCosts += sender -> m.latency
      } else {
        println("ERROR: Cost Message arrived from a non-Child")
      }
    }
    else {
      println("ERROR: Cost Message arrived at Host without Operator")
    }
    processedCostMessages += 1
    sendOutCostMessages()
  }

  private def sendOutCostMessages() : Unit = {
    if(children.isEmpty && costs.size == parentHosts.size){
      parentHosts.foreach(parent => parent ! CostMessage(costs(parent)._1))
    }
    else if (processedCostMessages == numberOfChildren && costs.size == parentHosts.size) {
      calculateOptimumNodes()
      val bottleNeckNode = minmaxBy(Minimizing, optimumHosts)(costs(_)._1)
      //minmaxBy(Minimizing, costs)(_._2._1)._1
      parentHosts.foreach(parent => parent ! CostMessage(mergeLatency(childCosts(bottleNeckNode), costs(parent)._1)))
      if (consumer) {
        broadcastMessage(StateTransferMessage(optimumHosts, node.get))
      }
    }
  }

  def calculateOptimumNodes() : Unit = {
    children.toSeq.foreach(child => optimumHosts = optimumHosts :+ minmaxBy(Minimizing, getChildAndTentatives(child._1))(costs(_)._1))
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

  def processStateTransferMessage(optimumHosts: Seq[ActorRef]): Unit ={
    if(optimumHosts.contains(self)){
      if(activeOperator.isDefined){
        if(children.nonEmpty){
          broadcastMessage(StateTransferMessage(optimumHosts, node.get))
        } else {
          parent.get ! MigrationComplete
        }
      }
      if(tentativeOperator.isDefined){
        activate()
        node.get ! Parent(parentNode.get)
        parent.get ! ChildResponse(node.get)
        broadcastMessage(StateTransferMessage(optimumHosts, node.get))
        children = Map.empty[ActorRef, Seq[ActorRef]]
        optimumHosts.foreach(host => children += host -> Seq.empty[ActorRef])
        resetAllData(false)
      }
    } else {
      resetAllData(true)
    }
  }

  def resetAllData(deleteEverything: Boolean): Unit ={
    if(deleteEverything){
      if(node.isDefined){
        node.get ! PoisonPill
      }
      node = None
      parent = None
      activeOperator = None
      tentativeOperator = None
      children = Map.empty[ActorRef, Seq[ActorRef]]
    }
    childNodes = Seq.empty[ActorRef]

    finishedChildren = 0
    processedCostMessages = 0
    tentativeHosts = Seq.empty[ActorRef]
    consumer = false
    ready = false
    receivedResponses = Set.empty
    optimumHosts = Seq.empty[ActorRef]
    costs = Map.empty
    operators = Map.empty[ActorRef, Option[Operator]]
  }

  def setOperators(sender: ActorRef, activeOperator: Option[ActiveOperator], tentativeOperator: Option[TentativeOperator]): Unit = {
    if (activeOperator.isDefined){
      operators += sender -> activeOperator
    }
    else
      operators += sender -> tentativeOperator
  }

  def becomeActiveOperator(operator: ActiveOperator): Unit ={
    if(!isOperator){
      activeOperator = Some(operator)
      node = Some(system.actorOf(activeOperator.get.props))
      neighbors.foreach(neighbor => neighbor ! OperatorResponse(activeOperator, None))
    }else{
      println("ERROR: Host already has an active Operator")
    }
  }

  def becomeTentativeOperator(operator: TentativeOperator): Unit ={
    if(!isOperator){
      tentativeOperator = Some(operator)
      neighbors.foreach(neighbor => neighbor ! OperatorResponse(None, tentativeOperator))
    } else {
      println("ERROR: Host already has a tentative Operator")
    }
  }

  def activate() : Unit = {
    if(tentativeOperator.isDefined){
      activeOperator = Some(ActiveOperator(tentativeOperator.get.host, tentativeOperator.get.props, tentativeOperator.get.dependencies))
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
