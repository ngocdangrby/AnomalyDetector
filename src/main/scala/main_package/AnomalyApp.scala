package main_package

;

import akka.actor.{ActorSystem, Props}
import stream_layer.{StartProcessing, StreamOutlierDetectSpark, StreamOutlierProcessActor};

object AnomalyApp {
  def main(args: Array[String]): Unit = {
    println("Start app")
    runProcess()
  }

  def runProcess(): Unit = {
    val actorSystem = ActorSystem("ActorSystem")

    val streamActor = actorSystem.actorOf(Props(new StreamOutlierProcessActor(new StreamOutlierDetectSpark)))

    streamActor ! StartProcessing
  }
}