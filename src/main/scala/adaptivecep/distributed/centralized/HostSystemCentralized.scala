package adaptivecep.distributed.centralized

import java.io.File

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory

object HostSystemCentralized{

  def main(args: Array[String]): Unit = {
    if (args.isEmpty)
      startup(Seq("application.conf"))
    else
      startup(args)
  }

  def startup(args: Seq[String]): Unit = {
      val file = new File(/*"fixedHosts/" + */args.head + ".conf")
      val config = ConfigFactory.parseFile(file).withFallback(ConfigFactory.load()).resolve()

      ActorSystem.create("ClusterSystem", config)
  }
}
