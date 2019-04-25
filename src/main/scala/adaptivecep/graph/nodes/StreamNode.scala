package adaptivecep.graph.nodes

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.qos._
import adaptivecep.publishers.Publisher._
import akka.{Done, NotUsed}
import akka.actor.{ActorRef, PoisonPill}
import akka.stream.{KillSwitches, OverflowStrategy, SourceRef, UniqueKillSwitch}
import akka.stream.scaladsl.{Keep, Sink, Source, SourceQueueWithComplete, StreamRefs}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

case class StreamNode(
    //query: StreamQuery,
    requirements: Set[Requirement],
    publisherName: String,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    bandwidthMonitorFactory: MonitorFactory,
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any])
  extends LeafNode {

  val publisher: ActorRef = publishers(publisherName)
  var subscriptionAcknowledged: Boolean = false
  var parentReceived: Boolean = false

  publisher ! Subscribe
  println("subscribing to publisher", publisher.path)


  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq.empty)
    case AcknowledgeSubscription(ref) if sender() == publisher =>
      subscriptionAcknowledged = true
      println("generated switch 1")
      killSwitch = Some(ref.viaMat(KillSwitches.single)(Keep.right).to(Sink foreach(e =>{
        emitEvent(e)
      })).run()(materializer))
      //if(parentReceived && !created) emitCreated()
    case Parent(p1) => {
      //println("Parent received", p1)
      parentNode = p1
      parentReceived = true
      nodeData = LeafNodeData(name, requirements, context, parentNode)
      //if (subscriptionAcknowledged && !created) emitCreated()
    }
    case CentralizedCreated =>
      if(!created){
        created = true
        emitCreated()
      }
    case SourceRequest =>
      queue = Source.queue[Event](20000, OverflowStrategy.backpressure)
        .viaMat(KillSwitches.single)(Keep.both).preMaterialize()(materializer)
      future = queue._2.runWith(StreamRefs.sourceRef())(materializer)
      sourceRef = Await.result(future, Duration.Inf)
      sender() ! SourceResponse(sourceRef)
    case KillMe =>
      if(killSwitch.isDefined){
      println("killed switch 1")
      killSwitch.get.shutdown()}
    case Kill =>
      //parentNode ! KillMe
      //self ! PoisonPill
      //fMonitor.scheduledTask.cancel()
      //println("Shutting down....")
    case Controller(c) =>
      controller = c
      //println("Got Controller", c)
    case CostReport(c) =>
      costs = c
      frequencyMonitor.onMessageReceive(CostReport(c), nodeData)
      latencyMonitor.onMessageReceive(CostReport(c), nodeData)
      bandwidthMonitor.onMessageReceive(CostReport(c), nodeData)
    case _: Event =>
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      bandwidthMonitor.onMessageReceive(unhandledMessage, nodeData)
  }

}
