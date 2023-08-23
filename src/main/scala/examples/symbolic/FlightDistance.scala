//package examples.symbolic
//
//import fuzzer.ProvInfo
//import org.apache.spark.{SparkConf, SparkContext}
//import provenance.data.Provenance
//import sparkwrapper.SparkContextWithDP
//import taintedprimitives.SymImplicits._
//import symbolicexecution.SymExResult
//import taintedprimitives.TaintedFloat
//
//object FlightDistance extends Serializable {
//  def main(args: Array[String]): SymExResult = {
//    val sparkConf = new SparkConf()
//
//    println("symbolic.FlightDistance called with following args:")
//    println(args.mkString("\n"))
//
//    if (args.length < 3) throw new IllegalArgumentException("Program was called with too few args")
//    sparkConf.setMaster(args(2))
//    sparkConf.setAppName(s"symbolic.FlightDistance ${args.mkString(",")}")//.set("spark.executor.memory", "2g")
//    val flights_data = args(0) // "datasets/fuzzing_seeds/FlightDistance/flights" // "/home/ahmad/Documents/VT/project1/cs5614-hw/data/flights"
//    val airports_data = args(1) // "datasets/fuzzing_seeds/FlightDistance/airports_data" // "/home/ahmad/Documents/VT/project1/cs5614-hw/data/airports_data"
//    val sc = SparkContext.getOrCreate(sparkConf)
//    val ctx = new SparkContextWithDP(sc)
//    ctx.setLogLevel("ERROR")
//    Provenance.setProvenanceType("dual")
//    val flights = ctx.textFileProv(flights_data, _.split(','))
//    val airports = ctx.textFileProv(airports_data, _.split(','))
//    val departure_flights = flights.map(r => (r(4), r(0)))
//    val arrival_flights = flights.map(r => (r(5), r(0)))
//    val airports_and_coords = airports.map(r => (r(0), (r(3), r(4))))
//    val dairports_and_coords = _root_.monitoring.Monitors.monitorJoinSymEx(departure_flights, airports_and_coords, 0)
//    val aairports_and_coords = _root_.monitoring.Monitors.monitorJoinSymEx(arrival_flights, airports_and_coords, 1)
//    val dflights_and_coords = dairports_and_coords.map({
//      case (ap, (id, (lat, long))) =>
//        (id, (ap, lat, long))
//    })
//    val aflights_and_coords = aairports_and_coords.map({
//      case (ap, (id, (lat, long))) =>
//        (id, (ap, lat, long))
//    })
//    val flights_and_coords = _root_.monitoring.Monitors.monitorJoinSymEx(dflights_and_coords, aflights_and_coords, 2)
//    val flights_and_distances = flights_and_coords.map({
//      case (fid, ((dap, dlat, dlong), (aap, alat, along))) =>
//        (fid, (dap, aap, distance((dlat.toFloat, dlong.toFloat), (alat.toFloat, along.toFloat))))
//    })
//    flights_and_distances.take(10).foreach(println)
//    _root_.monitoring.Monitors.finalizeSymEx()
//  }
//  def distance(departure: (TaintedFloat, TaintedFloat), arrival: (TaintedFloat, TaintedFloat)): Float = {
//    val R = 6373.0d
//    val (dlat, dlong) = departure
//    val (alat, along) = arrival
//    val (dlatr, dlongr) = (toRad(dlat), toRad(dlong))
//    val (alatr, alongr) = (toRad(alat), toRad(along))
//    val difflat = alatr - dlatr
//    val difflong = alongr - dlongr
//    val a = math.pow(math.sin(difflat / 2), 2) + math.cos(dlatr) * math.cos(alatr) * math.pow(math.sin(difflong / 2), 2)
//    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
//    (R * c * 0.621371d).toFloat
//  }
//  def toRad(d: TaintedFloat): TaintedFloat = {
//    d * math.Pi.toFloat / 180.0f
//  }
//}