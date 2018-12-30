package com.aeriaa.CO2FlightEmissions.messaging

import org.apache.kafka.clients.producer._
import java.util.Properties

import org.apache.log4j.{Level, Logger}


object FlightSender extends Serializable {



  Logger.getLogger("org").setLevel(Level.ERROR)



  def sendFlights(flights: String): Unit = {

    val  props = new Properties()

    val topics = Array ("flightsRealTime", "logFlightData")

    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("group.id", "flights-group")


    val kafkaProducer = new KafkaProducer[String,String](props)

    val record = new ProducerRecord(topics(0), "raw", flights)

    kafkaProducer.send(record)

    kafkaProducer.close()

  }


  def logData(data: String): Unit = {

    val  props = new Properties()

    val topics = Array ("flightsRealTime", "logFlightData")

    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("group.id", "flights-group")

    val kafkaProducer = new KafkaProducer[String,String](props)

    val record = new ProducerRecord(topics(1), "raw", data)

    kafkaProducer.send(record)

    kafkaProducer.close()

  }
}
