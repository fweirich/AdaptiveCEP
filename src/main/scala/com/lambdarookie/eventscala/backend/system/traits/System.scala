package com.lambdarookie.eventscala.backend.system.traits

import akka.actor.ActorRef
import com.lambdarookie.eventscala.backend.data.QoSUnits._
import com.lambdarookie.eventscala.backend.qos.QualityOfService.Violation
import com.lambdarookie.eventscala.data.Queries.Query
import rescala._

/**
  * Created by monur.
  */
trait System extends CEPSystem with QoSSystem


trait CEPSystem {
  val hosts: Signal[Set[Host]]

  private val operatorsVar: Var[Set[Operator]] = Var(Set.empty)
  private val nodesToOperatorsVar: Var[Map[ActorRef, Operator]] = Var(Map.empty)

  val operators: Signal[Set[Operator]] = operatorsVar
  val nodesToOperators: Signal[Map[ActorRef, Operator]] = nodesToOperatorsVar

  /**
    * Select the best host for a given operator
    * @param operator Operator, whose host we are seeking
    * @return Selected host
    */
  def selectHostForOperator(operator: Operator): Host // TODO: Operator placement strategy

  /**
    * Get the host of a node. Every node is mapped to an operator and therefore a host
    * @param node Node, whose host we are seeking
    * @return Given node's host
    */
  def getHostByNode(node: ActorRef): Host = nodesToOperators.now.get(node) match {
    case Some(operator) => operator.host
    case None => throw new NoSuchElementException("ERROR: Following node is not defined in the system: " + node)
  }

  /**
    * Add a node-operator pair to the system's [[nodesToOperators]] signal
    * @param node ActorRef of a node as key
    * @param operator Operator as value
    */
  def addNodeOperatorPair(node: ActorRef, operator: Operator): Unit =
    nodesToOperatorsVar.transform(x => x + (node -> operator))

  /**
    * Add operator to the system's [[operators]] signal
    * @param operator Operator to add
    */
  def addOperator(operator: Operator): Unit = operatorsVar.transform(x => x + operator)

  /**
    * Measure the frequency of each host
    */
  def measureFrequencies(): Unit = hosts.now.foreach(_.measureFrequency())

  /**
    * Find the lowest latency between hosts using Dijkstra's shortest path algorithm
    */
  def measureLowestLatencies(): Unit = hosts.now.foreach(host => {
    host.measureNeighborLatencies()
    var dests = hosts.now - host
    var nexts = host.neighbors
    while(nexts.nonEmpty) {
      val n = nexts.head
      nexts = nexts.tail
      if(host.lastLatencies.contains(n) && dests.contains(n)) {
        dests -= n
        nexts ++= n.neighbors.intersect(dests)
        n.neighbors.foreach(nn => if(n.lastLatencies.contains(nn) && (!host.lastLatencies.contains(nn)
          || (host.lastLatencies.contains(nn) && host.lastLatencies(nn) > host.lastLatencies(n) + n.lastLatencies(nn))))
          host.lastLatencies += (nn -> (host.lastLatencies(n) + n.lastLatencies(nn))))
      }
    }
  })

  /**
    * Find the highest bandwidth between hosts using a modified version of Dijkstra's algorithm
    */
  def measureHighestBandwidths(): Unit = hosts.now.foreach(host => {
    host.measureNeighborBandwidths()
    var dests = hosts.now - host
    var nexts = host.neighbors
    while(nexts.nonEmpty) {
      val n = nexts.head
      nexts = nexts.tail
      if(host.lastBandwidths.contains(n) && dests.contains(n)) {
        dests -= n
        nexts ++= n.neighbors.intersect(dests)
        n.neighbors.foreach(nn => if(n.lastBandwidths.contains(nn) && (!host.lastBandwidths.contains(nn)
          || (host.lastBandwidths.contains(nn)
          &&  host.lastBandwidths(nn) < min(host.lastBandwidths(n), n.lastBandwidths(nn)))))
          host.lastBandwidths += (nn -> min(host.lastBandwidths(n), n.lastBandwidths(nn))))
      }
    }
  })

  /**
    * Find the highest throughput between hosts using a modified version of Dijkstra's algorithm
    */
  def measureHighestThroughputs(): Unit = hosts.now.foreach(host => {
    host.measureNeighborThroughputs()
    var dests = hosts.now - host
    var nexts = host.neighbors
    while(nexts.nonEmpty) {
      val n = nexts.head
      nexts = nexts.tail
      if(host.lastThroughputs.contains(n) && dests.contains(n)) {
        dests -= n
        nexts ++= n.neighbors.intersect(dests)
        n.neighbors.foreach(nn => if(n.lastThroughputs.contains(nn) && (!host.lastThroughputs.contains(nn)
          || (host.lastThroughputs.contains(nn)
          && host.lastThroughputs(nn) < min(host.lastThroughputs(n), n.lastThroughputs(nn)))))
          host.lastThroughputs += (nn -> min(host.lastThroughputs(n), n.lastThroughputs(nn))))
      }
    }
  })
}


trait QoSSystem {
  private val queriesVar: Var[Set[Query]] = Var(Set.empty)
  private val fireDemandViolated: Evt[Violation] = Evt[Violation]

  val queries: Signal[Set[Query]] = queriesVar
  val demandViolated: Event[Violation] = fireDemandViolated

  def addQuery(query: Query): Unit = queriesVar.transform(x => x + query)

  def fireDemandViolated(violation: Violation): Unit = fireDemandViolated fire violation
}