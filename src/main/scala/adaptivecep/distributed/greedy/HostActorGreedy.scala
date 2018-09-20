package adaptivecep.distributed.greedy

import adaptivecep.data.Events._
import adaptivecep.distributed.HostActorDecentralizedBase
import adaptivecep.distributed.operator.Operator
import akka.actor.ActorRef

import scala.concurrent.duration._

class HostActorGreedy extends HostActorDecentralizedBase{

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
      parentHosts.foreach(parent => parent ! CostMessage(mergeLatency(childCosts(bottleNeckNode)._1, costs(parent)._1),
        mergeBandwidth(childCosts(bottleNeckNode)._2, costs(parent)._2)))
      if (consumer) {
        broadcastMessage(StateTransferMessage(optimumHosts, node.get))
      }
    }
    //println(children.isEmpty, processedCostMessages, numberOfChildren, costs.size, parentHosts.size)
  }

  def calculateOptimumNodes() : Unit = {
    //println(childHost1)
    //println(childHost2)

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
        //println(optimumChildHost1)
        //println(getPreviousChild(host))
      } else if(childHost2.isDefined && getPreviousChild(host) == childHost2.get){
        optimumChildHost2 = Some(host)
        //println(optimumChildHost2)
        //println(getPreviousChild(host))
      } else {
        println("ERROR: optimumHost does not belong to a child")
      }
    )
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