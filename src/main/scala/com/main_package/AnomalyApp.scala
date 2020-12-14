package com.main_package;

import akka.actor.{ActorSystem, Props}
import com.stream_layer.{StartProcessing, StreamOutlierDetectSpark, StreamOutlierProcessActor}

object AnomalyApp {
  def main(args: Array[String]): Unit = {
    println("Start app")
    runProcess(args(0), args(1), args(2), args(3))
  }

  def runProcess(kafkahost:String, prefixTopic:String, checkpointpath: String, colName: String): Unit = {
    println("hello")
    val actorSystem = ActorSystem("ActorSystem")
    println("hello2")
    val streamActor = actorSystem.actorOf(Props(new StreamOutlierProcessActor(new StreamOutlierDetectSpark(kafkahost, prefixTopic, checkpointpath, colName))))

    streamActor ! StartProcessing
  }
}