package testfiles

import abstraction.BaseRDD
import runners.Config
import utils.{QueryResult, RDDLocations}

object Test3 {


  def main(args: Array[String]): Unit = {
//
//    val ds0 = Seq(
//      "1,2,3",
//      "5,6,7",
//      "8,9,10"
//    )
//
//    val ds1 = Seq(
//      "a,b,c",
//      "d,e,f",
//      "g,h,i",
//      "g,x,y",
//      "g,w,t"
//    )
//
//    val filterColA = 1
//    val filterColB = 0
//    val filterColC = 2
//    val fqrA = ds0.filter(c => c.split(',')(filterColA).equals("2"))
//    val fqrB = ds1.filter(c => c.split(',')(filterColB).equals("g"))
//    val fqrC = ds1.filter(c => c.split(',')(filterColC).equals("f"))
//
//    val queryRDDList = List(
//      Array(new BaseRDD[String](fqrA), new BaseRDD[String](Seq())),
//      Array(new BaseRDD[String](Seq()), new BaseRDD[String](fqrB)),
//      Array(new BaseRDD[String](Seq()), new BaseRDD[String](fqrC))
//    )
//
//    val locs = Seq(
//      new RDDLocations(Array((0, filterColA, 0))),
//      new RDDLocations(Array((1, filterColB, 2))),
//      new RDDLocations(Array((1, filterColC, 1)))
//    )
//
//    val queryResultList = queryRDDList.zip(locs).map{
//      case (qr, loc) => new QueryResult(qr, Seq(), loc)
//    }
//
//    val result = queryResultList.foldLeft(queryResultList.head){case (acc, e) => acc.mixMatchQueryResult(e, "random")}
//    println(result)
  }

}
