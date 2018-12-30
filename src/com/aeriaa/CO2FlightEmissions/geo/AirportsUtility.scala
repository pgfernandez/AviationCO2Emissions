package com.aeriaa.CO2FlightEmissions.geo

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object AirportsUtility {


  private val sparkSession = SparkSession.builder()
    .appName("airports")
    .config("spark.master" ,"local[*]")
    .getOrCreate()


  private val dataAirports = sparkSession.read
    .option("header", "true")
    .option("inferSchema", value = true)
    .csv("in/airports.csv")

  dataAirports.persist(StorageLevel.MEMORY_ONLY)

  def calculateRouteDistance (fromAirport: String, toAirport: String): Map[String, Any] = {

    val coordinatesFrom = dataAirports.select("latitude_deg", "longitude_deg", "iso_country" )
                          .filter("ident == '" + fromAirport + "'" )
    val coordinatesTo = dataAirports.select("latitude_deg", "longitude_deg", "iso_country" )
                            .filter("ident == '" + toAirport + "'" )


    if (coordinatesFrom.collect().length > 0 && coordinatesTo.collect().length > 0) {

      val fromLatitude = coordinatesFrom.head().getDouble(0)
      val fromLongitude = coordinatesFrom.head().getDouble(1)
      val fromCountry = coordinatesFrom.head().getString(2)

      val toLatitude = coordinatesTo.head().getDouble(0)
      val toLongitude = coordinatesTo.head().getDouble(1)
      val toCountry = coordinatesTo.head().getString(2)


      val distance = GreatCircleDistance.calculateGCD(fromLatitude, fromLongitude, toLatitude, toLongitude)


      val result = Map("distance" -> distance.toInt, "fromCountry" -> fromCountry.toString, "toCountry" -> toCountry.toString)

      result

    }else{

      val result = Map("distance" -> 0, "fromCountry" -> "null", "toCountry" -> "null")
      result
    }

  }












}
