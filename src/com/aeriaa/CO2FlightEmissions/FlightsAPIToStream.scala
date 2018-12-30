package com.aeriaa.CO2FlightEmissions

import org.json4s._
import org.json4s.jackson.JsonMethods._


import org.apache.log4j.{Level, Logger}
import com.aeriaa.CO2FlightEmissions.messaging.FlightSender

import java.io.{File, PrintWriter}


object FlightsAPIToStream{

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    // Brings in default date formats etc.
    implicit val formats = DefaultFormats

    //lo que quiero ir agrupando para enviar a los diferentes tópicos de Kafka

    //all the fields for the flight
    /*case class Flight(icao: String, aircraftRegistration: String, firstSeen: BigInt, trackedSeconds: BigInt,
                      messagesBroacasted: BigInt, altitude: Int, latitude: Double, longitude: Double,
                      positionTime: BigInt, speed: Int, trackAngle: Double, trackHeading: Boolean,
                      aircraftModel: String, manufacturer: String, year: Int, aircraftSerialNumber: String,
                      operatedBy: String, operatorIcao: String, verticalSpeed: Int, wakeTurbulenceCategory: Int,
                      aircraftSpecie: Int, engineType: Int, numberOfEngines: String, countryAircrfatIcao: String,
                      from: String, to: String, grounded: Boolean, flightCallsign: String, hasPic: Boolean,
                      targetAltitude: Int, autopilotTrack: Int, waypoints: List[Waypoint])


    case class Flight3(Id: BigInt, Rcvr: Int, HasSig: Boolean, Sig: Int, Icao: String, Reg: String, Fseen: BigInt,
                      Tsecs: Int, Cmsgs: Int, Alt: Int, Galt: Int, InHG: Double, AltT: Boolean, Lat: Double, Long: Double,
                      PosTime: BigInt, Mlat: Boolean, TisB: Boolean, Spd: Double, SpdTyp: Int, Trak: Double, TrkH: Boolean,
                      Type: String, Mdl: String, Man: String, Year: Int, Cnum: String, Op: String, OpIcao: String,
                      Sqk: Int, Vsi: Int, VsiT: Boolean, WTC: Int, Species: Int, EngType: Int, EngMount: Int,
                      Engines: String, Mil: Boolean, Cou: String, From: String, To: String, Gnd: Boolean, Call: String,
                      CallSus: Boolean, HasPic: Boolean, FlightsCount: Int, Interested: Boolean, Help: Boolean,
                      Trt: Int, TT: String, ResetTrail: Boolean, Talt: Int, Ttrk: Int, Sat: Boolean, PosStale: Boolean,
                      Source: String)

    case class Flight(Id: BigInt, Rcvr: Int, HasSig: Boolean, Sig: Int, Icao: String, Bad: Boolean, Reg: String, Fseen: BigInt,
                      Tsecs: Int, Cmsgs: Int, Alt: Int, Galt: Int, InHG: Double, AltT: Boolean, Lat: Double, Long: Double,
                      PosTime: BigInt, Mlat: Boolean, TisB: Boolean, Spd: Double, SpdTyp: Int, Trak: Double, TrkH: Boolean,
                      Type: String, Mdl: String, Man: String, Year: Int, Cnum: String, Op: String, OpIcao: String,
                      Sqk: Int, Vsi: Int, VsiT: Boolean, WTC: Int, Species: Int, EngType: Int, EngMount: Int,
                      Engines: String, Mil: Boolean, Cou: String, From: String, To: String, Gnd: Boolean, Call: String,
                      CallSus: Boolean, HasPic: Boolean, FlightsCount: Int, Interested: Boolean, Help: Boolean,
                      Trt: Int, TT: String, ResetTrail: Boolean, Talt: Int, Ttrk: Int, Sat: Boolean, PosStale: Boolean,
                      Source: String, Cos: List[Waypoint])



    case class Fligths(flights: List[Flight])

    case class Waypoint(latitude: Double, longitude: Double, time: BigInt, altitude: Int)

    case class Route(from: String, to: String, firstSeen: BigInt, trackedSeconds: BigInt,
                     messagesBroacasted: BigInt, altitude: Int, latitude: Double, longitude: Double,
                     positionTime: BigInt, speed: Int, operatedBy: String, operatorIcao: String, verticalSpeed: Int,
                     grounded: Boolean, flightCallsign: String, hasPic: Boolean, targetAltitude: Int,
                     autopilotTrack: Int, aircraftModel: String, manufacturer: String)

    case class Aircraft (aircraftRegistration: String, aircraftModel: String, manufacturer: String, year: Int,
                         aircraftSerialNumber: String, hasPic: Boolean, urlPic: String, engineType: Int,
                         numberOfEngines: String, countryAircrfatIcao: String, consumptionPerHourPerEngine: Int, co2perHour: Int)*/

    @throws(classOf[java.io.IOException])
    @throws(classOf[java.net.SocketTimeoutException])
    def get(url: String,
            connectTimeout: Int = 15000,
            readTimeout: Int = 15000,
            requestMethod: String = "GET") =
    {
      import java.net.{URL, HttpURLConnection}
      val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
      connection.setConnectTimeout(connectTimeout)
      connection.setReadTimeout(readTimeout)
      connection.setRequestMethod(requestMethod)
      connection.setRequestProperty("User-Agent", """NING/1.0""")

        val inputStream = connection.getInputStream

        val content = scala.io.Source.fromInputStream(inputStream).mkString
        if (inputStream != null) inputStream.close
          content //return

    }


    def parseFlight(content: JValue) :Unit = {

      val flights = content \\ "acList" //para los de proximidad

      val dataList = flights.extract[JArray]


      val list = dataList.extract[List[Map[String, Any]]]

      val writer = new PrintWriter(new File("out/data/flights_" + System.currentTimeMillis() + ".json"))

      val flightEmitter = FlightSender

      list.foreach(
        flight => {

          val flightToSend = org.json4s.jackson.Serialization.write(flight)
          writer.write(flightToSend)



          //val flightsToSend = compact(render(dataList))

          flightEmitter.sendFlights(flightToSend)
          writer.write(",")

        })

      flightEmitter.sendFlights("EOF")

      writer.close()
    }

    try {
      //val content = get("https://public-api.adsbexchange.com/VirtualRadar/AircraftList.json?lat=40.416775&lng=-3.703790&fDstL=0&fDstU=100")
      println("****** INICIANDO LA PETICIÓN DE VUELOS ************")

      val content = get("https://public-api.adsbexchange.com/VirtualRadar/AircraftList.json?trFmt=sa")

      println("********* PETICIÓN DE VUELOS CORRECTA **************")

      implicit val formats = DefaultFormats.strict

      val json = org.json4s.jackson.parseJson(content)

      parseFlight(json)

    } catch {
      case ioe: java.io.IOException =>  // handle this
      case ste: java.net.SocketTimeoutException => // handle this
    }


  }

}
