package com.aeriaa.CO2FlightEmissions.geo


object GreatCircleDistance {

//Based on Robert Sedgewick and Kevin Wayne https://introcs.cs.princeton.edu/java/12types/GreatCircle.java

def calculateGCD (fromLatitude: Double, fromLongitude: Double, toLatitude: Double, toLongitude: Double): Int = {

  val x1 = Math.toRadians(fromLatitude.toDouble)
  val y1 = Math.toRadians(fromLongitude.toDouble)
  val x2 = Math.toRadians(toLatitude.toDouble)
  val y2 = Math.toRadians(toLongitude.toDouble)

  // great circle distance in radians
  var angle1 = Math.acos(Math.sin(x1) * Math.sin(x2) + Math.cos(x1) * Math.cos(x2) * Math.cos(y1 - y2))
  // convert back to degrees
  angle1 = Math.toDegrees(angle1)
  // each degree on a great circle of Earth is 60 nautical miles
  val distance = 60 * angle1

  //convert from nautical miles to km

  val distanceKm = distance * 1.852

  distanceKm.toInt

}
}
