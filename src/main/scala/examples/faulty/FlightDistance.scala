package examples.faulty
import abstraction.{SparkConf, SparkContext}
import capture.IOStreams._println

object FlightDistance {

  def main(args: Array[String]): Unit = {
    println(s"synthetic 3 args ${args.mkString(",")}")
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("Column Provenance Test")//.set("spark.executor.memory", "2g")
    val flights_data = args(0) //"datasets/fuzzing_seeds/FlightDistance/flights"
    val airports_data = args(1) //"datasets/fuzzing_seeds/FlightDistance/airports_data"
    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    ctx.setLogLevel("ERROR")
    val flights = ctx.textFile(flights_data).map(_.split(','))
    val airports = ctx.textFile(airports_data).map(_.split(','))
    val departure_flights = flights.map(r => (r(4), r(0))) // (DME, 1185)
    val arrival_flights = flights.map(r => (r(5), r(0))) // (BTK, 1185)
    val airports_and_coords = airports.map(r => (r(0), (r(3), r(4)))) // (YKS, (129.777, 62.093))
    val dairports_and_coords = departure_flights.join(airports_and_coords) // (DME,(1185,(37.9062995910645,55.4087982177734)))
    val aairports_and_coords = arrival_flights.join(airports_and_coords) // (NSK,(1546,(87.3321990966797,69.3110961914062)))
    val dflights_and_coords = dairports_and_coords.map{case (ap, (id, (lat, long))) => (id, (ap, lat, long))} //(12032,(KZN,49.278701782227,55.606201171875))
    val aflights_and_coords = aairports_and_coords.map{case (ap, (id, (lat, long))) => (id, (ap, lat, long))} //(12032,(KZN,49.278701782227,55.606201171875))
    val flights_and_coords = dflights_and_coords.join(aflights_and_coords) //(25604,((ULV,48.2266998291,54.2682991028),(DME,37.9062995910645,55.4087982177734)))
    val flights_and_distances = flights_and_coords.map{
      case (fid, ((dap, dlat, dlong), (aap, alat, along))) =>
        (fid, (dap,aap,distance((dlat.toFloat,dlong.toFloat),(alat.toFloat,along.toFloat))))
    }
    flights_and_distances.collect().take(10).foreach(_println)
  }

  def distance(departure: (Float, Float), arrival: (Float, Float)): Float = {
    val R = 6373.0
    val (dlat, dlong) = departure
    val (alat, along) = arrival
    val (dlatr, dlongr) = (toRad(dlat), toRad(dlong))
    val (alatr, alongr) = (toRad(alat), toRad(along))
    val difflat = alatr-dlatr
    val difflong = alongr-dlongr
    val a = math.pow(math.sin(difflat / 2), 2) + math.cos(dlatr) * math.cos(alatr) * math.pow(math.sin(difflong / 2),2)
    val c = 2*math.atan2(math.sqrt(a), math.sqrt(1-a)) // should be 1-a. Results in NaNs
    val dist = (R * c * 0.621371).toFloat
    dist
  }

  def toRad(d: Float): Float = {
    (d * math.Pi/180.0).toFloat
  }

}