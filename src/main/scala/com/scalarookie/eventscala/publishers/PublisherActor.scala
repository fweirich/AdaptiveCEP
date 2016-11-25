package com.scalarookie.eventscala.publishers

import akka.actor.{Actor, ActorRef}

import scala.concurrent.duration.FiniteDuration
import scala.util.Random

object PublisherActor {

  case object Subscribe
  case object Unsubscribe

}

trait PublisherActor extends Actor {

  var subscribers: Set[ActorRef] =
    scala.collection.immutable.Set.empty

  override def receive: Receive = {
    case Subscribe =>
      this.subscribers = this.subscribers + sender
    case Unsubscribe =>
      this.subscribers = this.subscribers - sender
  }

}
