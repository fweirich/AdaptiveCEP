package com.lambdarookie.eventscala.graph.monitors

import java.util.concurrent.TimeUnit

import akka.actor.{ActorContext, ActorRef, ActorSystem}
import com.lambdarookie.eventscala.data.Events._
import com.lambdarookie.eventscala.data.Queries._
import com.lambdarookie.eventscala.backend.system.traits.System

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global

trait NodeData { val name: String; val query: Query; val system: System; val context: ActorContext }

case class LeafNodeData(name: String, query: Query, system: System,
                        context: ActorContext) extends  NodeData
case class UnaryNodeData(name: String, query: Query, system: System,
                         context: ActorContext, childNode: ActorRef) extends NodeData
case class BinaryNodeData(name: String, query: Query, system: System,
                          context: ActorContext, childNode1: ActorRef, childNode2: ActorRef) extends NodeData

trait NodeMonitor {
  def onCreated(nodeData: NodeData): Unit = ()
  def onEventEmit(event: Event, nodeData: NodeData): Unit = ()
  def onMessageReceive(message: Any, nodeData: NodeData): Unit = ()
}

case class GraphMonitor(interval: Int) {
  def onCreated(system: System, actorSystem: ActorSystem): Unit = actorSystem.scheduler.schedule(
    initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
    interval = FiniteDuration(interval, TimeUnit.SECONDS),
    runnable = () => {
      system.measureFrequencies()
      system.measureLowestLatencies()
      system.measureHighestBandwidths()
      system.measureHighestThroughputs()
    })
}
