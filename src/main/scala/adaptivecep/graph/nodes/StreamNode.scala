package adaptivecep.graph.nodes

import adaptivecep.data.Events.{Delay, _}
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.qos._
import adaptivecep.publishers.Publisher._
import akka.actor.{ActorRef, PoisonPill}

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

  def moveTo(a: ActorRef): Unit = {
    a ! Parent(parentNode)
    parentNode ! ChildUpdate(self, a)
  }

  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq.empty)
    case AcknowledgeSubscription if sender() == publisher =>
      subscriptionAcknowledged = true
      //if(parentReceived && !created) emitCreated()
    case Parent(p1) => {
      //println("Parent received", p1)
      parentNode = p1
      parentReceived = true
      nodeData = LeafNodeData(name, requirements, context, parentNode)
      //if (subscriptionAcknowledged && !created) emitCreated()
    }
    case event: Event if sender() == publisher =>
      emitEvent(event)
    case CentralizedCreated =>
      if(!created){
        created = true
        emitCreated()
      }
    case Move(a) => {
      moveTo(a)
    }
    case KillMe => sender() ! PoisonPill
    case Kill =>
      self ! PoisonPill
      //println("Shutting down....")
    case Controller(c) =>
      controller = c
      //println("Got Controller", c)
    case HostPropsResponse(c) =>
      costs = c
      frequencyMonitor.onMessageReceive(HostPropsResponse(c), nodeData)
      latencyMonitor.onMessageReceive(HostPropsResponse(c), nodeData)
      bandwidthMonitor.onMessageReceive(HostPropsResponse(c), nodeData)
    case _: Event =>
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      bandwidthMonitor.onMessageReceive(unhandledMessage, nodeData)
  }

}
