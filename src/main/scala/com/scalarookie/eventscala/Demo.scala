package com.scalarookie.eventscala

import akka.actor.{ActorSystem, Props}
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.dsl._
import com.scalarookie.eventscala.graph._
import com.scalarookie.eventscala.publishers._

object Demo extends App {

  val actorSystem = ActorSystem()

  val publisherA = actorSystem.actorOf(Props(
    RandomPublisher((timestamp, id) => Event2[Integer, String](timestamp, (id, id.toString)))), "A")
  val publisherB = actorSystem.actorOf(Props(
    RandomPublisher((timestamp, id) => Event2[String, Integer](timestamp, (id.toString, id)))), "B")
  val publisherC = actorSystem.actorOf(Props(
    RandomPublisher((timestamp, id) => Event1[java.lang.Boolean](timestamp, if (id % 2 == 0) true else false))), "C")

  val publishers = Map("A" -> publisherA, "B" -> publisherB, "C" -> publisherC)

  val subquery: Query =
    stream[String, Integer].from("B")
    .select(elements(2))

  val query: Query =
    stream[Integer, String].from("A")
    .join(subquery).in(slidingWindow(3 instances), tumblingWindow(3 seconds))
    .join(stream[java.lang.Boolean].from("C")).in(slidingWindow(1 instances), slidingWindow(1 instances))
    .select(elements(1, 2, 4))
    .where(element(1) <:= literal(15))
    .where(literal(true) =:= element(3))

  val query2: Query =
    stream[Integer, String].from("A")
      .join(stream[Integer, String].from("A"))
      .in(tumblingWindow(1 instances), tumblingWindow(1 instances))

  val graph = actorSystem.actorOf(Props(
    new RootNode(query2, publishers, event => println(s"Complex event received: $event"))),
    "root")

}
