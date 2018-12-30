package com.aeriaa.CO2FlightEmissions.analytics


import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import com.aeriaa.CO2FlightEmissions.geo.AirportsUtility
import com.aeriaa.CO2FlightEmissions.messaging.FlightSender

object FlightsBasicAnalysis {


  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    val ssc = new StreamingContext("local[*]", "realtime-flight",Seconds(95))


    //creación de sesión
    val sparkSession = SparkSession.builder()
      .appName("co2-fuel-analysis")
      .config("spark.master" ,"local[*]")
      .getOrCreate()


    ssc.checkpoint("out/checkpoint")


    //parametros kafka
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "flights-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )

    //me subscribo al tópico
    val topics = Array("flightsRealTime")

    //crear el DStream
    val stream = KafkaUtils.createDirectStream[String,String](ssc,PreferConsistent, Subscribe[String,String](topics,kafkaParams))

    //capturamos el valor del mensaje proveniente de kafka
    val flightsRT = stream.map(flightData => flightData.value)




    flightsRT.foreachRDD(
      rdd => {


        println("paso por aqui, a ver cuántas")

        if (rdd.count() > 0) {

          val flights = sparkSession.read.json(rdd)
          val flightLogEmitter = FlightSender

        // val procesado = flights.select("Man", "From", "To", "Spd", "Alt").groupBy("Man").avg("Spd", "Alt")
       // val procesado = flights.select("Man", "From", "To", "Spd", "Alt").groupBy("From").avg("Spd", "Alt")
       //  val procesado = flights.groupBy("Man").agg(avg("Spd").alias("1"))

      val routes = flights.select("From", "To", "Type", "Call", "Man", "Op", "OpIcao", "Engines", "Reg", "Cou", "Year")
                    .filter(col("From").notEqual("null") && col("To").notEqual("null"))

        println("numero de rutas con aeropuertos: " + routes.count())

        routes.foreach( row => {


            //REVISAR QUE TENEMOS DATOS

            val fromAirport = row.getString(0).trim.slice(0,4)
            val toAirport = row.getString(1).trim.slice(0,4)
            val aircraftType = row.getString(2)
            val callSign = row.getString(3)
            val aircraftManufacturer =  row.getString(4)
            val operatedBy = row.getString(5)
            val operatedByIcao = row.getString(6)
            val engines = row.getString(7)
            val aircraftRegistration = row.getString(8)
            val airlineCountry = row.getString(9)
            val aircraftYear = row.getString(10)
            var co2FlightData:Map[String, Double] = Map()


            val geoResults =  AirportsUtility.calculateRouteDistance(fromAirport, toAirport)

            val distance = geoResults.getOrElse("distance", 0).asInstanceOf[Int]

            val fromCountry = geoResults.getOrElse("fromCountry", "null").asInstanceOf[String]
            val toCountry = geoResults.getOrElse("toCountry", "null").asInstanceOf[String]


            if (distance > 0) {
              if (aircraftType != null) {

                co2FlightData = CO2FlightCalculator.CO2Calculator(aircraftType, distance)

              }else{

                co2FlightData += ("noInfo" -> 0.0)

              }

            }else{

              co2FlightData += ("noInfo" -> 0.0)

            }

          var logFlightData = callSign + ";" + operatedBy + ";" + operatedByIcao + ";" + airlineCountry + ";" +
            fromAirport + ";" + toAirport + ";" + distance + ";" + aircraftType + ";"+ aircraftManufacturer+ ";" +
            engines + ";" + aircraftYear + ";" + fromCountry + ";" + toCountry + ";"

          if (co2FlightData.contains("noInfo")){

            logFlightData += co2FlightData("noInfo").toDouble + ";" +  co2FlightData("noInfo").toDouble + ";" +
              co2FlightData("noInfo").toDouble + ";"

          }else {

            logFlightData += co2FlightData("perPax").toDouble + ";" + co2FlightData("total").toDouble + ";" +
              co2FlightData("seatsFactorized").toDouble
          }

          flightLogEmitter.logData(logFlightData)

          })

          flightLogEmitter.logData("EOF")

        }else{
          println("aun no he recibido nada")
        }
      }

    )

    //escucha activa
    ssc.start()
    ssc.awaitTermination()


  }




}
