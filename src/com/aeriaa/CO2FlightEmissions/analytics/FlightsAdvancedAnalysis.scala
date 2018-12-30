package com.aeriaa.CO2FlightEmissions.analytics


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object FlightsAdvancedAnalysis {

  def main(args: Array[String]): Unit = {

    //configuración nivel de log
    Logger.getLogger("org").setLevel(Level.ERROR)

    //creación de sesión
    val sparkSession = SparkSession.builder()
      .appName("CO2-analysis-data")
      .config("spark.master" ,"local[*]")
      .getOrCreate()

    //permite convertir objetos de Scala en Dataframe
    import sparkSession.implicits._


    val dataBatch = sparkSession.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .option("delimiter", ";")
      .csv("out/data/CO2Data/flightsCO2_1535804078175.csv")




    val countries = dataBatch.select("AirlineCountry", "CO2PerPax").groupBy("AirlineCountry").avg("CO2PerPax").orderBy("avg(CO2PerPax)")

    //escribo los resultados en un fichero usando solo una partición
    countries.repartition(1).write.csv("out/analysis/CO2PerPax_PerCountry.csv")

    val aircraft = dataBatch.select("AircraftType", "CO2PerPax").groupBy("AircraftType").avg("CO2PerPax").orderBy("avg(CO2PerPax)")

    aircraft.repartition(1).write.csv("out/analysis/CO2PerPax_PerAircraft.csv")

    val distanceLocal = dataBatch.select("Distance", "CO2PerPax", "From", "To", "OperatedBy", "AircraftType").filter($"Distance" <= 700).orderBy("CO2PerPax")

    distanceLocal.repartition(1).write.csv("out/analysis/CO2PerPax_DistanceLocal.csv")

    val distanceMedium = dataBatch.select("Distance", "CO2PerPax", "From", "To", "OperatedBy", "AircraftType").filter($"Distance" > 700 && $"Distance" <= 3000).orderBy("CO2PerPax")

    distanceMedium.repartition(1).write.csv("out/analysis/CO2PerPax_DistanceMedium.csv")

    val distanceLarge = dataBatch.select("Distance", "CO2PerPax", "From", "To", "OperatedBy", "AircraftType").filter($"Distance" > 3000).orderBy("CO2PerPax")

    distanceLarge.repartition(1).write.csv("out/analysis/CO2PerPax_DistanceLarge.csv")

    val destination = dataBatch.select("To", "CO2PerPax").groupBy("To").avg("CO2PerPax").orderBy("avg(CO2PerPax)")

    destination.repartition(1).write.csv("out/analysis/CO2PerPax_Destination.csv")

    val origin = dataBatch.select("From", "CO2PerPax").groupBy("From").avg("CO2PerPax").orderBy("avg(CO2PerPax)")

    origin.repartition(1).write.csv("out/analysis/CO2PerPax_Origin.csv")

    val airline = dataBatch.select("OperatedBy", "OperatedByICAO", "CO2PerPax")
      .groupBy("OperatedBy").avg("CO2PerPax").orderBy("avg(CO2PerPax)")

    airline.repartition(1).write.csv("out/analysis/CO2PerPax_Airline.csv")

    val fromCountry = dataBatch.select("FromCountry", "CO2PerPax")
      .groupBy("FromCountry").avg("CO2PerPax").orderBy("avg(CO2PerPax)")

    fromCountry.repartition(1).write.csv("out/analysis/CO2PerPax_FromCountry.csv")

    val toCountry = dataBatch.select("ToCountry", "CO2PerPax")
      .groupBy("ToCountry").avg("CO2PerPax").orderBy("avg(CO2PerPax)")

    fromCountry.repartition(1).write.csv("out/analysis/CO2PerPax_ToCountry.csv")

//puedo hacer un toJSON y enviarlo por kafka a......




    sparkSession.close()

  }

}
