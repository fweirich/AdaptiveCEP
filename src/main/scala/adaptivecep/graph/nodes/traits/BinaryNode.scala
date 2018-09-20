package adaptivecep.graph.nodes.traits

import java.util.concurrent.TimeUnit

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.dsl.Dsl.stream
import adaptivecep.graph.qos._
import akka.actor.{ActorRef, Cancellable}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, FiniteDuration}

trait BinaryNode extends Node {

  val createdCallback: Option[() => Any]
  val eventCallback: Option[(Event) => Any]

  val query: Query1[Int] = stream[Int]("A")

  val interval = 2
  var badCounter = 0
  var goodCounter = 0

  var childNode1: ActorRef = self
  var childNode2: ActorRef = self

  var parentNode: ActorRef = self

  var frequencyMonitor: BinaryNodeMonitor = frequencyMonitorFactory.createBinaryNodeMonitor
  var latencyMonitor: BinaryNodeMonitor = latencyMonitorFactory.createBinaryNodeMonitor
  var bandwidthMonitor: BinaryNodeMonitor = bandwidthMonitorFactory.createBinaryNodeMonitor
  var nodeData: BinaryNodeData = BinaryNodeData(name, requirements, context, childNode1, childNode2, parentNode)
  var scheduledTask: Cancellable = _

  val lmonitor: PathLatencyBinaryNodeMonitor = latencyMonitor.asInstanceOf[PathLatencyBinaryNodeMonitor]
  val bmonitor: PathBandwidthBinaryNodeMonitor = bandwidthMonitor.asInstanceOf[PathBandwidthBinaryNodeMonitor]

  override def preStart(): Unit = {
    if(scheduledTask == null){
      scheduledTask = context.system.scheduler.schedule(
      initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
      interval = FiniteDuration(interval, TimeUnit.SECONDS),
      runnable = () => {
        //val pathLatency1 = latencyMonitor.asInstanceOf[PathLatencyBinaryNodeMonitor].childNode1PathLatency
        //val pathLatency2 = latencyMonitor.asInstanceOf[PathLatencyBinaryNodeMonitor].childNode2PathLatency
        if(lmonitor.latency.isDefined && bmonitor.bandwidthForMonitoring.isDefined) {
          if(!lmonitor.met || !bmonitor.met){
            goodCounter = 0
            badCounter += 1
            if(badCounter >= 3){
              badCounter = 0
              controller ! RequirementsNotMet
            }
          }
          if(lmonitor.met && bmonitor.met){
            goodCounter += 1
            badCounter = 0
            if(goodCounter >= 3){
              controller ! RequirementsMet
            }
          }
          println(lmonitor.latency.get.toNanos/1000000.0 + ", " + bmonitor.bandwidthForMonitoring.get)
          lmonitor.latency = None
          bmonitor.bandwidthForMonitoring = None
        }
      }
  )}}

  override def postStop(): Unit = {
    scheduledTask.cancel()
    lmonitor.scheduledTask.cancel()
    bmonitor.scheduledTask.cancel()
    println("Shutting down....")
  }

  def emitCreated(): Unit = {
    lmonitor.childNode1 = childNode1
    lmonitor.childNode2 = childNode2
    bmonitor.childNode1 = childNode1
    bmonitor.childNode2 = childNode2
    if (createdCallback.isDefined) createdCallback.get.apply() //else parentNode ! Created
    frequencyMonitor.onCreated(nodeData)
    latencyMonitor.onCreated(nodeData)
    bandwidthMonitor.onCreated(nodeData)
  }

  def emitEvent(event: Event): Unit = {
    lmonitor.childNode1 = childNode1
    lmonitor.childNode2 = childNode2
    bmonitor.childNode1 = childNode1
    bmonitor.childNode2 = childNode2
    if (eventCallback.isDefined) eventCallback.get.apply(event) else parentNode ! event
    frequencyMonitor.onEventEmit(event, nodeData)
    latencyMonitor.onEventEmit(event, nodeData)
    bandwidthMonitor.onEventEmit(event, nodeData)
  }

  def setDelay(b: Boolean): Unit = {
    delay = b
    //lmonitor.delay = b
  }

}