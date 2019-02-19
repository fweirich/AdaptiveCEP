package adaptivecep.distributed.annealing


import java.util.concurrent.TimeUnit

import adaptivecep.data.Cost.Cost
import adaptivecep.data.Events._
import adaptivecep.distributed.HostActorDecentralizedBase
import adaptivecep.distributed.operator.{NodeHost, Operator, TentativeOperator}
import akka.actor.ActorRef

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/*
class HostActorAnnealing extends HostActorDecentralizedBase {

  val minTemperature: Double = 0.01
  var temperature: Double = 1.0
  val temperatureReductionFactor: Double = 0.9
  var temperatureCounter = 0


  override def processEvent(event: PlacementEvent, sender: ActorRef): Unit ={
    event match {
      case BecomeTentativeOperator(operator, p, pHosts, c1, c2, t) =>
        temperature = t
      case Start =>
        if(temperature > minTemperature) {
          temperature = temperature * temperatureReductionFactor
        }
      case RequirementsNotMet =>
        temperatureCounter = 0
        if(temperature > minTemperature) {
          temperature = temperature * temperatureReductionFactor
        }
      case RequirementsMet =>
        temperatureCounter += 1
        if(consumer && temperature != 1.0 && temperatureCounter > 2){
          temperature = 1.0
          broadcastMessage(ResetTemperature)
        }
      case ResetTemperature =>
        temperature = 1.0
        if(activeOperator.isDefined){
          broadcastMessage(ResetTemperature)
        }
      case _ =>
    }
    super.processEvent(event, sender)
  }

  override def chooseTentativeOperators() : Unit = {
    //println("CHOOSING TENTATIVE OPERATORS")
    if (children.nonEmpty || parent.isDefined){
      if(activeOperator.isDefined){
        val neighborSeq: Seq[ActorRef] = neighbors.toSeq
        var timeout = 0
        var chosen: Boolean = false
        while (tentativeHosts.size < degree && timeout < 1000 && !chosen){
          val randomNeighbor =  neighborSeq(random.nextInt(neighborSeq.size))
          if(operators(randomNeighbor).isEmpty && !tentativeHosts.contains(randomNeighbor)){
            val tenOp = TentativeOperator(NodeHost(randomNeighbor), activeOperator.get.props, activeOperator.get.dependencies)
            send(randomNeighbor, BecomeTentativeOperator(tenOp, parentNode.get, parentHosts, childHost1, childHost2, temperature))
            chosen = true
          }
          timeout += 1
        }
        if(timeout >= 1000){
          //println("Not enough hosts available as tentative Operators. Continuing without...")
          send(children.toSeq.head._1, ChooseTentativeOperators(tentativeHosts :+ self))
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

  def sendOutCostMessages() : Unit = {
    if(children.isEmpty && latencyResponses.size == parentHosts.size && bandwidthResponses.size == parentHosts.size){
      parentHosts.foreach(parent => parent ! CostMessage(costs(parent).duration, costs(parent).bandwidth))
    }
    else if (processedCostMessages == numberOfChildren && latencyResponses.size == parentHosts.size && bandwidthResponses.size == parentHosts.size) {
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
      parentHosts.foreach(parent => parent ! CostMessage(mergeLatency(childCosts(bottleNeckNode)._1, costs(parent).duration),
        mergeBandwidth(childCosts(bottleNeckNode)._2, costs(parent).bandwidth)))
      if (consumer) {
        broadcastMessage(StateTransferMessage(optimumHosts, node.get))
      }
    }
    //println(children.isEmpty, processedCostMessages, numberOfChildren, costs.size, parentHosts.size)
  }

  def calculateOptimumNodes() : Unit = {
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
          optimumChildHost2 = worse2
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
          diff = (childCosts(tChild)._2).-(childCosts(child._1)._2)
        } else {
          diff = (childCosts(child._1)._2 + 1 / childCosts(child._1)._1.toMillis).-(childCosts(tChild)._2 + 1 / childCosts(tChild)._1.toMillis)
        }
        var acceptanceProb = 0.0
        if(diff < 0){
          acceptanceProb = Math.exp(diff/temperature)
        }
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

  /*def resetAllData(deleteEverything: Boolean): Unit ={
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

    optimumHosts = Seq.empty[ActorRef]
    tentativeHosts = Seq.empty[ActorRef]

    finishedChildren = 0
    completedChildren = 0
    processedCostMessages = 0
    receivedResponses = Set.empty[ActorRef]
    latencyResponses = Set.empty[ActorRef]
    bandwidthResponses = Set.empty[ActorRef]

    ready = false

    operators = Map.empty[ActorRef, Option[Operator]]
  }*/
}
*/