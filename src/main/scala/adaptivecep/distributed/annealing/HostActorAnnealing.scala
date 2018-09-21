package adaptivecep.distributed.annealing

import java.util.concurrent.TimeUnit

import adaptivecep.data.Events._
import adaptivecep.distributed.HostActorDecentralizedBase
import adaptivecep.distributed.operator.Operator
import akka.actor.ActorRef

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class HostActorAnnealing extends HostActorDecentralizedBase {

  val minTemperature: Double = 0.01
  var temperature: Double = 1.0
  val temperatureReductionFactor: Double = 0.9

  override def processEvent(event: GreedyPlacementEvent, sender: ActorRef): Unit ={
    event match {
      case m: CostMessage => processCostMessage(m, sender)
      case BecomeActiveOperator(operator) =>
        becomeActiveOperator(operator)
      case SetActiveOperator(operator) =>
        setActiveOperator(operator)
      case BecomeTentativeOperator(operator, p, pHosts, c1, c2) =>
        parentNode = Some(p)
        parentHosts = pHosts
        childHost1 = c1
        childHost2 = c2
        becomeTentativeOperator(operator, sender)
      case BecomeTentativeOperatorWithTemperature(operator, p, pHosts, c1, c2, t) =>
        parentNode = Some(p)
        parentHosts = pHosts
        childHost1 = c1
        childHost2 = c2
        temperature = t
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
        //println("Choosing Tentative Operators")
        parentHosts = parents
        if(parents.isEmpty){
          consumer = true
          //println(children)
          children.toSeq.head._1 ! ChooseTentativeOperators(Seq(self))
        } else if(children.isEmpty){
          parentHosts.foreach(_ ! FinishedChoosing(Seq.empty[ActorRef]))
        } else {
          setup()
        }
      case OperatorRequest =>
        //println("Got Operator Request, Sending Response", isOperator, " to", sender)
        sender ! OperatorResponse(activeOperator, tentativeOperator)
      case OperatorResponse(ao, to) =>
        receivedResponses += sender
        //println("Received Operator Response:")
        //println("no. of responses ", receivedResponses.size)
        //println("no. of neighbors ", neighbors.size)
        if(ao.isDefined){
          operators += sender -> ao
        } else {
          operators += sender -> to
        }
        if(receivedResponses.size == neighbors.size){
          chooseTentativeOperators()
        }
      case ChildResponse(c) =>
        //println("Got Child Response", c)
        //println(childHost1)
        //println(childHost2)
        if(childHost1.isDefined && sender.equals(childHost1.get)){
          childNode1 = Some(c)
          hostToNodeMap += childHost1.get -> c
        } else if (childHost2.isDefined && sender.equals(childHost2.get)){
          childNode2 = Some(c)
          hostToNodeMap += childHost2.get -> c
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
        //println("Got Parent",p ,ref)
        if(node.isDefined){
          hostToNodeMap += p -> ref
          reportCostsToNode()
          node.get ! Parent(ref)
          parentNode = Some(ref)
          parent = Some(p)
        }
      case FinishedChoosing(tChildren) =>
        children += sender -> tChildren
        finishedChildren += 1
        //println("A child finished choosing tentative operators", finishedChildren, "/", children.size)
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
        if(delay){
          parentHosts.foreach(p => p ! CostRequest(clock.instant().minusMillis(40)))
        } else {
          parentHosts.foreach(p => p ! CostRequest(clock.instant()))
        }
        if (activeOperator.isDefined) {
          broadcastMessage(Start)
        }
      case CostRequest(t) =>
        system.scheduler.scheduleOnce(
          FiniteDuration(hostPropsToMap(sender).duration.toMillis, TimeUnit.MILLISECONDS),
          () => {sender ! CostResponse(t, hostPropsToMap(sender).bandwidth)})
      case CostResponse(t, _) =>
        if (parentHosts.contains(sender)) {
          costs += sender -> (FiniteDuration(java.time.Duration.between(t, clock.instant).dividedBy(2).toMillis, TimeUnit.MILLISECONDS), hostPropsToMap(sender).bandwidth)
          sendOutCostMessages()
        }
      case StateTransferMessage(o, p) =>
        parent = Some(sender)
        parentNode = Some(p)
        hostToNodeMap += sender -> p
        processStateTransferMessage(o)
      case RequirementsNotMet =>
        println(sender)
        if(consumer && ready){
          ready = false
          //println("RECALCULATING")
          broadcastMessage(Start)
        }
      case RequirementsMet =>
        if(temperature != 1.0){
          //println("Resetting Temperature....")
          temperature = 1.0
          broadcastMessage(RequirementsMet)
        }
      case MigrationComplete =>
        //println("A Child completed Migration", sender)
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

  def sendOutCostMessages() : Unit = {
    if(children.isEmpty && costs.size == parentHosts.size){
      parentHosts.foreach(parent => parent ! CostMessage(costs(parent)._1, costs(parent)._2))
    }
    else if (processedCostMessages == numberOfChildren && costs.size == parentHosts.size) {
      calculateOptimumNodes()
      //println(optimumHosts)
      var bottleNeckNode = self
      if(optimizeFor == "latency"){
        bottleNeckNode = minmaxBy(Maximizing, optimumHosts)(childCosts(_)._1)
      }else if(optimizeFor == "bandwidth"){
        bottleNeckNode = minmaxBy(Minimizing, optimumHosts)(childCosts(_)._2)
      }else{
        bottleNeckNode = minmaxBy(Minimizing, optimumHosts)(childCosts(_))
      }
      //println(bottleNeckNode)
      childHost1 = optimumChildHost1
      childHost2 = optimumChildHost2
      optimumChildHost1 = None
      optimumChildHost2 = None
      //minmaxBy(Minimizing, costs)(_._2._1)._1
      parentHosts.foreach(parent => parent ! CostMessage(mergeLatency(childCosts(bottleNeckNode)._1, costs(parent)._1), mergeBandwidth(childCosts(bottleNeckNode)._2, costs(parent)._2)))
      if (consumer) {
        broadcastMessage(StateTransferMessage(optimumHosts, node.get))
        if(temperature > minTemperature){
          temperature = temperature * temperatureReductionFactor
        }
      }
    }
    //println(children.isEmpty, processedCostMessages, numberOfChildren, costs.size, parentHosts.size)
  }

  def calculateOptimumNodes() : Unit = {
    //println(childHost1)
    //println(childHost2)

    if(activeOperator.isDefined){
      val worseSolution = findWorseAcceptableSolution()
      if(childHost1.isDefined){
        val worse1 = containsWorseSolutionFor(childHost1.get, worseSolution)
        if (worse1.isDefined){
          optimumChildHost1 = worse1
          optimumHosts = optimumHosts :+ worse1.get
        }
        else {
          var opt1 = self
          if(optimizeFor == "latency"){
            opt1 = minmaxBy(Minimizing, getChildAndTentatives(childHost1.get))(childCosts(_)._1)
          }else if(optimizeFor == "bandwidth"){
            opt1 = minmaxBy(Maximizing, getChildAndTentatives(childHost1.get))(childCosts(_)._2)
          }else{
            opt1 = minmaxBy(Maximizing, getChildAndTentatives(childHost1.get))(childCosts(_))
          }
          optimumChildHost1 = Some(opt1)
          optimumHosts = optimumHosts :+ opt1
        }
      }
      if(childHost2.isDefined){
        val worse2 = containsWorseSolutionFor(childHost2.get, worseSolution)
        if (worse2.isDefined){
          optimumChildHost1 = worse2
          optimumHosts = optimumHosts :+ worse2.get
        }
        else {
          var opt2 = self
          if(optimizeFor == "latency"){
            opt2 = minmaxBy(Minimizing, getChildAndTentatives(childHost2.get))(childCosts(_)._1)
          }else if(optimizeFor == "bandwidth"){
            opt2 = minmaxBy(Maximizing, getChildAndTentatives(childHost2.get))(childCosts(_)._2)
          }else{
            opt2 = minmaxBy(Maximizing, getChildAndTentatives(childHost2.get))(childCosts(_))
          }
          optimumChildHost2 = Some(opt2)
          optimumHosts = optimumHosts :+ opt2
        }
      }
    }
    if(tentativeOperator.isDefined){
      if(optimizeFor == "latency"){
        children.toSeq.foreach(child => optimumHosts = optimumHosts :+ minmaxBy(Minimizing, getChildAndTentatives(child._1))(childCosts(_)._1))
      }else if(optimizeFor == "bandwidth"){
        children.toSeq.foreach(child => optimumHosts = optimumHosts :+ minmaxBy(Maximizing, getChildAndTentatives(child._1))(childCosts(_)._2))
      }else{
        children.toSeq.foreach(child => optimumHosts = optimumHosts :+ minmaxBy(Maximizing, getChildAndTentatives(child._1))(childCosts(_)))
      }

      optimumHosts.foreach(host =>
        if(childHost1.isDefined && getPreviousChild(host) == childHost1.get){
          optimumChildHost1 = Some(host)
         // println(optimumChildHost1)
          //println(getPreviousChild(host))
        } else if(childHost2.isDefined && getPreviousChild(host) == childHost2.get){
          optimumChildHost2 = Some(host)
         // println(optimumChildHost2)
         // println(getPreviousChild(host))
        } else {
          println("ERROR: optimumHost does not belong to a child")
        }
      )
    }
  }

  def containsWorseSolutionFor(actorRef: ActorRef, worseSolutions: Seq[ActorRef]): Option[ActorRef] = {
    worseSolutions.foreach(child => if(getPreviousChild(child) == actorRef) return Some(child))
    None
  }

  def findWorseAcceptableSolution(): Seq[ActorRef] = {
    //println("Finding Worse Solution")

    var result = Seq.empty[ActorRef]
    for(child <- children){
      var temp = Seq.empty[ActorRef]
      for (tChild <- child._2){
        var diff: Double = 0
        if(optimizeFor == "latency"){
          diff = childCosts(child._1)._1.-(childCosts(tChild)._1).toMillis
        } else if(optimizeFor == "bandwidth"){
          diff = childCosts(child._1)._2.-(childCosts(tChild)._2)
        } else {
          diff = (childCosts(child._1)._2 + 1 / childCosts(child._1)._1.toMillis).-(childCosts(tChild)._2 + 1 / childCosts(tChild)._1.toMillis)
        }
        val acceptanceProb = Math.exp(diff/temperature)
        if(acceptanceProb > Math.random()){
          temp = temp :+ tChild
        }
      }
      if(temp.nonEmpty)
        result = result :+ temp.head
    }
    //println(result)
    result
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
}