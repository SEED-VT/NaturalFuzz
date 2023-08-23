package examples.mutants.FlightDistance
import abstraction.{ SparkConf, SparkContext }
import capture.IOStreams._println
object FlightDistance_M5_40_times_plus {
  def main(args: Array[String]): Unit = {
    println(s"synthetic 3 args ${args.mkString(",")}")
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("Column Provenance Test")
    val flights_data = args(0)
    val airports_data = args(1)
    val ctx = new SparkContext(sparkConf)
    ctx.setLogLevel("ERROR")
    val flights = ctx.textFile(flights_data).map(_.split(','))
    val airports = ctx.textFile(airports_data).map(_.split(','))
    val departure_flights = flights.map(r => (r(4), r(0)))
    val arrival_flights = flights.map(r => (r(5), r(0)))
    val airports_and_coords = airports.map(r => (r(0), (r(3), r(4))))
    val dairports_and_coords = departure_flights.join(airports_and_coords)
    val aairports_and_coords = arrival_flights.join(airports_and_coords)
    val dflights_and_coords = dairports_and_coords.map({
      case (ap, (id, (lat, long))) =>
        (id, (ap, lat, long))
    })
    val aflights_and_coords = aairports_and_coords.map({
      case (ap, (id, (lat, long))) =>
        (id, (ap, lat, long))
    })
    val flights_and_coords = dflights_and_coords.join(aflights_and_coords)
    val flights_and_distances = flights_and_coords.map({
      case (fid, ((dap, dlat, dlong), (aap, alat, along))) =>
        (fid, (dap, aap, distance((dlat.toFloat, dlong.toFloat), (alat.toFloat, along.toFloat))))
    })
    flights_and_distances.collect().take(10).foreach(_println)
  }
  def distance(departure: (Float, Float), arrival: (Float, Float)): Float = {
    val R = 6373.0d
    val (dlat, dlong) = departure
    val (alat, along) = arrival
    val (dlatr, dlongr) = (toRad(dlat), toRad(dlong))
    val (alatr, alongr) = (toRad(alat), toRad(along))
    val difflat = alatr - dlatr
    val difflong = alongr - dlongr
    val a = math.pow(math.sin(difflat / 2), 2) + (math.cos(dlatr) + math.cos(alatr)) * math.pow(math.sin(difflong / 2), 2)
    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    val dist = (R * c * 0.621371d).toFloat
    dist
  }
  def toRad(d: Float): Float = {
    (d * math.Pi / 180.0d).toFloat
  }
}