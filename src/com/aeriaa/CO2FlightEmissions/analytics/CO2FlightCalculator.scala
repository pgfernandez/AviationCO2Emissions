package com.aeriaa.CO2FlightEmissions.analytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel


object CO2FlightCalculator {

  //creación de sesión
  private val sparkSession = SparkSession.builder()
    .appName("co2-fuel")
    .config("spark.master" ,"local[*]")
    .getOrCreate()

  //lectura del csv de los aeropuertos creando un Dataframe
  private val dataAircraft = sparkSession.read
    .option("header", "true")
    .option("inferSchema", value = true)
    .option("delimiter", ";")
    .csv("in/fuelConsumption.csv")

  dataAircraft.persist(StorageLevel.MEMORY_ONLY)

  def CO2Calculator (aircraftType: String, distance: Int): Map[String, Double] = {


    val aircraftData = dataAircraft.select("FuelSimple", "Seats")
                      .filter("Type == '" + aircraftType + "'")

    if (aircraftData.collect().length > 0) {

      var newDistance = 0

      if (distance < 550) {

        newDistance = distance + 50

      } else if (distance >= 550 && distance < 5500) {

        newDistance = distance + 100

      } else {

        newDistance = distance + 125
      }


      val fuelKgPerKm =  aircraftData.head.getDouble(0)


      //ICAO FORMULA: CO2 per pax = 3.16 * (total fuel * pax-to-freight factor) / (number of economy seats * pax load factor
      val seatsFactorized = aircraftData.head.getString(1).toInt * 0.80
      val CO2PerPax = 3.16 * newDistance * fuelKgPerKm / 1000 * 0.85 / seatsFactorized

      val totalCO2 = CO2PerPax * (seatsFactorized)

      val co2Results = Map("perPax" -> CO2PerPax, "total" -> totalCO2, "seatsFactorized" -> seatsFactorized)

      co2Results

    }else{

       Map("noInfo" -> 0.0)

    }

  }


}
