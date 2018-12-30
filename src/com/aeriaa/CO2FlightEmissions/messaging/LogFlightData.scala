package com.aeriaa.CO2FlightEmissions.messaging


import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

import java.io.{File, PrintWriter}

object LogFlightData {

  def main(args: Array[String]): Unit = {

    val TOPIC="logFlightData"

    val  props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "flights-group")

    val consumer = new KafkaConsumer[String, String](props)

    consumer.subscribe(util.Collections.singletonList(TOPIC))

    var newFile = true

    val writer = new PrintWriter(new File("out/data/CO2Data/flightsCO2_" + System.currentTimeMillis() + ".csv"))
    val head = "CallSign;OperatedBy;OperatedByICAO;AirlineCountry;From;To;Distance;AircraftType;AircraftManufacturer;NumberOfEngines;AircraftYear;FromCountry;ToCountry;CO2PerPax;CO2FlightTotal;FlightSeats"

    writer.write(head + "\n")

    while(true){

      val records=consumer.poll(100)

      for (record<-records.asScala){


        if(record.value().equals("EOF")){
          writer.flush()
          writer.close()


        }else{
          writer.write(record.value() + "\n")
        }

        println(record)


      }
    }


  }




}