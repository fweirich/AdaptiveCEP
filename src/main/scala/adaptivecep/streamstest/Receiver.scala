package adaptivecep.streamstest

import adaptivecep.publishers.Publisher.{AcknowledgeSubscription, Subscribe}
import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.dispatch.{BoundedMessageQueueSemantics, RequiresMessageQueue}
import akka.stream.{ActorMaterializer, KillSwitches, SourceRef, UniqueKillSwitch}
import akka.stream.scaladsl.{Keep, Sink}
import adaptivecep.data.Events.{Event, Kill}

object Receiver

case class Receiver(actorRef: ActorRef, actorRef2: ActorRef) extends Actor with ActorLogging with RequiresMessageQueue[BoundedMessageQueueSemantics]{

  val materializer = ActorMaterializer()
  var killSwitch: Option[UniqueKillSwitch] = None

  override def preStart(): Unit = {
    super.preStart()
    actorRef ! Subscribe
    println("subscribing")
  }
  def receive: Receive = {
    //case AcknowledgeSubscription(ref) => ref.getSource.to(Sink foreach println).run(materializer)
    case AcknowledgeSubscription(ref) =>
      println("Ack")
      killSwitch = Some(ref.viaMat(KillSwitches.single)(Keep.right).to(Sink foreach(e =>{
        println(e)
      })).run()(materializer))
    case event: Event => println(event + "direct")
    case Kill =>
      killSwitch.get.shutdown()
      Thread.sleep(3000)
      actorRef ! Subscribe
  }
}
