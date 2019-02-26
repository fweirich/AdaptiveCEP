package adaptivecep.distributed.annealing


import adaptivecep.data.Cost.Cost
import adaptivecep.data.Events._
import adaptivecep.distributed.operator.{NodeHost, TentativeOperator}
import adaptivecep.distributed.{HostActorDecentralizedBase, Stage}
import akka.actor.ActorRef
import rescala.default._


class HostActorAnnealing extends HostActorDecentralizedBase {

  val minTemperature: Double = 0.01
  var temperature: Double = 1.0
  val temperatureReductionFactor: Double = 0.9
  var temperatureCounter = 0


  setTemperature observe(temp => temperature = temp)
  demandViolated observe{_ =>
    temperatureCounter = 0
  if(temperature > minTemperature) {
    temperature = temperature * temperatureReductionFactor
  }}
  demandNotViolated observe{_ =>  temperatureCounter += 1
    if(consumer && temperature != 1.0 && temperatureCounter > 2){
      temperature = 1.0
      broadcastMessage(ResetTemperature)
    }
  }
  resetTemperature observe{_ =>  temperature = 1.0
    if(activeOperator.isDefined){
      broadcastMessage(ResetTemperature)
    }}
  makeTentativeOperator observe{host => send(host, temperature)}
/*
  override def processEvent(event: PlacementEvent, sender: ActorRef): Unit ={
    event match {
      case BecomeTentativeOperator(operator, p, pHosts, c1, c2, t) =>
        temperature = t
      case Start =>
        if(temperature > minTemperature) {
          temperature = temperature * temperatureReductionFactor
        }
      case RequirementsNotMet(_) =>
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
  */

  /*
  override def chooseTentativeOperators() : Unit = {
    println("CHOOSING TENTATIVE OPERATORS")
    if (children.now.nonEmpty || parent.isDefined){
      if(activeOperator.isDefined){
        var timeout = 0
        var chosen: Boolean = false
        while (tentativeHosts.size < degree && timeout < 1000 && !chosen){
          val randomNeighbor =  hosts.now.toVector(random.nextInt(hosts.now.size))
          if(!reversePlacement.now.contains(randomNeighbor) && !tentativeHosts.contains(randomNeighbor)){
            val tenOp = TentativeOperator(activeOperator.get.props, activeOperator.get.dependencies)
            send(randomNeighbor, BecomeTentativeOperator(tenOp, parentNode.get, parentHosts, childHost1, childHost2, temperature))
            chosen = true
          }
          timeout += 1
        }
        if(timeout >= 1000){
          //println("Not enough hosts available as tentative Operators. Continuing without...")
          send(children.now.toSeq.head._1, ChooseTentativeOperators(tentativeHosts + thisHost))
        }
        //children.toSeq.head._1 ! ChooseTentativeOperators(tentativeHosts :+ self)
      } else {
        println("ERROR: Only Active Operator can choose Tentative Operators")
      }
    }
    else {
      stage.set(Stage.Measurement)
      parentHosts.foreach(send(_, FinishedChoosing(tentativeHosts)))
    }
  }

*/

  def calculateOptimumHosts(children: Map[NodeHost, Set[NodeHost]],
                            accumulatedCost: Map[NodeHost, Cost],
                            childHost1: Option[NodeHost],
                            childHost2: Option[NodeHost]): Seq[NodeHost] = {

    var result: Seq[NodeHost] = Seq.empty[NodeHost]
    var optimum: Seq[NodeHost] = Seq.empty[NodeHost]
    if(activeOperator.isDefined){
      val worseSolution = findWorseAcceptableSolution(children, accumulatedCost)
      if(childHost1.isDefined){
        val worse1 = containsWorseSolutionFor(childHost1.get, worseSolution, children)
        if (worse1.isDefined){
          //optimumChildHost1 = worse1
          result = result :+ worse1.get
        }
        else {
          var opt1 = thisHost
          if(optimizeFor == "latency"){
            opt1 = minmaxBy(Minimizing, getChildAndTentatives(childHost1.get, children))(accumulatedCost(_).duration)
          }else if(optimizeFor == "bandwidth"){
            opt1 = minmaxBy(Maximizing, getChildAndTentatives(childHost1.get, children))(accumulatedCost(_).bandwidth)
          }else{
            opt1 = minmaxBy(Maximizing, getChildAndTentatives(childHost1.get, children))(x => (accumulatedCost(x).duration, accumulatedCost(x).bandwidth))
          }
          //optimumChildHost1 = Some(opt1)
          result = result :+ opt1
        }
      }
      if(childHost2.isDefined){
        val worse2 = containsWorseSolutionFor(childHost2.get, worseSolution, children)
        if (worse2.isDefined){
          result = result :+ worse2.get
        }
        else {
          var opt2 = thisHost
          if(optimizeFor == "latency"){
            opt2 = minmaxBy(Minimizing, getChildAndTentatives(childHost2.get, children))(accumulatedCost(_).duration)
          }else if(optimizeFor == "bandwidth"){
            opt2 = minmaxBy(Maximizing, getChildAndTentatives(childHost2.get, children))(accumulatedCost(_).bandwidth)
          }else{
            opt2 = minmaxBy(Maximizing, getChildAndTentatives(childHost2.get, children))(x => (accumulatedCost(x).duration, accumulatedCost(x).bandwidth))
          }
          //optimumChildHost2 = Some(opt2)
          result = result :+ opt2
        }
      }
    }
    if(tentativeOperator.isDefined){
      if(optimizeFor == "latency"){
        children.toSeq.foreach(child => optimum = optimum :+ minmaxBy(Minimizing, getChildAndTentatives(child._1, children))(accumulatedCost(_).duration))
      }else if(optimizeFor == "bandwidth"){
        children.toSeq.foreach(child => optimum = optimum :+ minmaxBy(Maximizing, getChildAndTentatives(child._1, children))(accumulatedCost(_).bandwidth))
      }else{
        children.toSeq.foreach(child => optimum = optimum :+ minmaxBy(Maximizing, getChildAndTentatives(child._1, children))(x => (accumulatedCost(x).duration, accumulatedCost(x).bandwidth)))
      }

      optimum.foreach(host =>
        if(childHost1.isDefined && getPreviousChild(host, children) == childHost1.get){
          result = result :+ host
         // println(optimumChildHost1)
          //println(getPreviousChild(host))
        } else if(childHost2.isDefined && getPreviousChild(host, children) == childHost2.get){
          result = result :+ host
         // println(optimumChildHost2)
         // println(getPreviousChild(host))
        } else {
          println("ERROR: optimumHost does not belong to a child")
        }
      )
    }
    //println(result)
    result
  }

  def containsWorseSolutionFor(host: NodeHost, worseSolutions: Set[NodeHost], children: Map[NodeHost, Set[NodeHost]]): Option[NodeHost] = {
    worseSolutions.foreach(child => if(getPreviousChild(child, children) == host) return Some(child))
    None
  }

  def findWorseAcceptableSolution(children : Map[NodeHost, Set[NodeHost]], accumulatedCost: Map[NodeHost, Cost]): Set[NodeHost] = {
    //println("Finding Worse Solution")

    var result = Set.empty[NodeHost]
    for(child <- children){
      var temp = Seq.empty[NodeHost]
      for (tChild <- child._2){
        var diff: Double = 0
        if(optimizeFor == "latency"){
          diff = accumulatedCost(child._1).duration.-(accumulatedCost(tChild).duration).toMillis
        } else if(optimizeFor == "bandwidth"){
          diff = accumulatedCost(tChild).bandwidth.-(accumulatedCost(child._1).bandwidth)
        } else {
          diff = (accumulatedCost(child._1).bandwidth + 1 / accumulatedCost(child._1).duration.toMillis).-(accumulatedCost(tChild).bandwidth + 1 / accumulatedCost(tChild).duration.toMillis)
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
        result = result + temp.head
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
