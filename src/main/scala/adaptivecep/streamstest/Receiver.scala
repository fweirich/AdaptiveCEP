package adaptivecep.streamstest

import adaptivecep.publishers.Publisher.{AcknowledgeSubscription, Subscribe}
import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.dispatch.{BoundedMessageQueueSemantics, RequiresMessageQueue}
import akka.stream.{ActorMaterializer, SourceRef}
import akka.stream.scaladsl.Sink
import adaptivecep.data.Events.Event

object Receiver

case class Receiver(actorRef: ActorRef) extends Actor with ActorLogging with RequiresMessageQueue[BoundedMessageQueueSemantics]{

  val materializer = ActorMaterializer()

  override def preStart(): Unit = {
    super.preStart()
    actorRef ! Subscribe
    println("subscribing")
  }
  def receive: Receive = {
    //case AcknowledgeSubscription(ref) => ref.getSource.to(Sink foreach println).run(materializer)
    case ref: SourceRef[Event] =>
      ref.getSource.to(Sink foreach println).run(materializer)
  }
}
