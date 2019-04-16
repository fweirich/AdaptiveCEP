package adaptivecep.publishers

import adaptivecep.data.Events.Event
import adaptivecep.publishers.Publisher._
import akka.NotUsed
import akka.actor.{Actor, ActorRef}
import akka.remote.WireFormats.TimeUnit
import akka.stream.scaladsl.{Sink, Source, SourceQueueWithComplete, StreamRefs}
import akka.stream.{ActorMaterializer, OverflowStrategy, SourceRef}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

trait Publisher extends Actor {
  import akka.pattern.pipe

  val materializer = ActorMaterializer()

  val source: (SourceQueueWithComplete[Event], Source[Event, NotUsed]) = Source.queue[Event](1000, OverflowStrategy.backpressure).preMaterialize()(materializer)
  val future: Future[SourceRef[Event]] = source._2.runWith(StreamRefs.sourceRef())(materializer)


  var subscribers: Set[ActorRef] =
    scala.collection.immutable.Set.empty[ActorRef]

  override def receive: Receive = {
    case Subscribe =>
      subscribers = subscribers + sender()
      //pipe(future).to(sender())
      sender ! AcknowledgeSubscription(Await.result(future, Duration.Inf))
  }

}

object Publisher {

  case object Subscribe
  case class AcknowledgeSubscription(source: SourceRef[Event])
  case class Something(sourceRef: SourceRef[Event])

}
