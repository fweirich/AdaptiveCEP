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
  var count = 0

  override def preStart(): Unit = {
    super.preStart()
    actorRef ! Subscribe
    println("subscribing")
  }
  def receive: Receive = {
    //case AcknowledgeSubscription(ref) => ref.getSource.to(Sink foreach println).run(materializer)
    case AcknowledgeSubscription(ref) =>
      ref.getSource.to(Sink foreach(e => {
        count += 1
        if(count == 1000){
          println(1000)
          count = 0
        }
      })).run(materializer)
    case event: Event => println(event + "direct")

  }
}
