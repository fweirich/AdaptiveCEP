package adaptivecep.publishers

import adaptivecep.data.Events.Event
import adaptivecep.publishers.Publisher._
import akka.NotUsed
import akka.actor.{Actor, ActorRef}
import akka.remote.WireFormats.TimeUnit
import akka.stream.scaladsl.{Keep, Sink, Source, SourceQueueWithComplete, StreamRefs}
import akka.stream._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

trait Publisher extends Actor {
  import akka.pattern.pipe

  val materializer = ActorMaterializer()

  var source: ((SourceQueueWithComplete[Event], UniqueKillSwitch), Source[Event, NotUsed]) = Source.queue[Event](20000, OverflowStrategy.dropNew)
    .viaMat(KillSwitches.single)(Keep.both).preMaterialize()(materializer)
  var future: Future[SourceRef[Event]] = source._2.runWith(StreamRefs.sourceRef())(materializer)

  var subscribers: Set[ActorRef] =
    scala.collection.immutable.Set.empty[ActorRef]

  override def receive: Receive = {
    case Subscribe =>
      subscribers = subscribers + sender()
      //pipe(future).to(sender())
      source = Source.queue[Event](20000, OverflowStrategy.dropNew)
        .viaMat(KillSwitches.single)(Keep.both).preMaterialize()(materializer)
      future = source._2.runWith(StreamRefs.sourceRef())(materializer)
      sender ! AcknowledgeSubscription(Await.result(future, Duration.Inf))
  }

}

object Publisher {

  case object Subscribe
  case class AcknowledgeSubscription(source: SourceRef[Event])
  case class Something(sourceRef: SourceRef[Event])

}
