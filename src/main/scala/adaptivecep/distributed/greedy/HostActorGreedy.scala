package adaptivecep.distributed.greedy

import adaptivecep.data.Cost.Cost
import adaptivecep.data.Events._
import adaptivecep.distributed.HostActorDecentralizedBase
import adaptivecep.distributed.operator.{Host, NodeHost, Operator}



class HostActorGreedy extends HostActorDecentralizedBase{

  def calculateOptimumHosts(children: Map[NodeHost, Set[NodeHost]],
                            accumulatedCost: Map[NodeHost, Cost],
                            childHost1: Option[NodeHost],
                            childHost2: Option[NodeHost]) : Seq[NodeHost] = {
    var optimum: Seq[NodeHost] = Seq.empty[NodeHost]
    var result: Seq[NodeHost] = Seq.empty[NodeHost]
    if(optimizeFor == "latency"){
      children.toSeq.foreach(child => optimum = optimum :+ minmaxBy(Minimizing,
        getChildAndTentatives(child._1, children))(accumulatedCost(_).duration))
    }else if(optimizeFor == "bandwidth"){
      children.toSeq.foreach(child => optimum = optimum :+ minmaxBy(Maximizing,
        getChildAndTentatives(child._1, children))(accumulatedCost(_).bandwidth))
    }else{
      children.toSeq.foreach(child => optimum = optimum :+ minmaxBy(Maximizing,
        getChildAndTentatives(child._1, children))(x => (accumulatedCost(x).duration, accumulatedCost(x).bandwidth)))
    }

    optimum.foreach(host =>
      if(childHost1.isDefined && getPreviousChild(host, children) == childHost1.get) {
        result = result :+ host
      }
    )
    optimum.foreach(host =>
      if(childHost2.isDefined && getPreviousChild(host, children) == childHost2.get){
        result = result :+ host
      }
    )
    result
  }
}
