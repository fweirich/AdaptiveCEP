package adaptivecep.graph.nodes

import adaptivecep.data.Events._
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
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any])
  extends LeafNode {

  val publisher: ActorRef = publishers(publisherName)
  //val publisher: ActorRef = publishers(query.publisherName)

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
      emitCreated()
    case event: Event if sender() == publisher =>
      emitEvent(event)
    case Parent(p1) => {
      parentNode = p1
      println("parent received stream", self.path)
    }
    case Move(a) => {
      moveTo(a)
    }
    case KillMe => sender() ! PoisonPill
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }

}
