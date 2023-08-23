package schemas

import fuzzer.{Guidance, Schema}
import scoverage.Coverage
import utils.FileUtils


object BenchmarkSchemas {
  val SYNTHETIC1 = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL)
    ),
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL)
    )
  )

  val SYNTHETIC2 = Array[Array[Schema[Any]]](
    // Dataset 1: Alfreds,Maria,Germany,1,1
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL)
    ),
    // Dataset 2: 10308,2,1996-09-18
    Array(
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_OTHER)
    )
  )

  val SYNTHETIC3 = Array[Array[Schema[Any]]](
    // Dataset 1: 1185,PG0134,"2017-09-10 02:50:00.000","2017-09-10 07:55:00.000",DME,BTK,Scheduled,319,,
    Array(
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_CATEGORICAL),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_CATEGORICAL),
      new Schema(Schema.TYPE_CATEGORICAL),
      new Schema(Schema.TYPE_CATEGORICAL),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER)
    ),
    // Dataset 2: YKS,"Yakutsk Airport",Yakutsk,129.77099609375,62.0932998657227,Asia/Yakutsk
    Array(
      new Schema(Schema.TYPE_CATEGORICAL),
      new Schema(Schema.TYPE_CATEGORICAL),
      new Schema(Schema.TYPE_CATEGORICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_CATEGORICAL)
    )
  )

  val SEGMENTATION = Array[Array[Schema[Any]]](
    // http://www.youtube.com,1000,100,100,920,2,advertisement
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_OTHER)
    ),
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_OTHER)
    )
  )

  val COMMUTE = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL)
    ),
    Array(
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL)
    )
  )

  val DELAYS = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_OTHER)
    ),
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_OTHER)
    )
  )

  val CUSTOMERS = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER)
    ),
    Array(
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_OTHER)
    )
  )

  val FAULTS = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL)
    )
  )

  val STUDENTGRADE = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL)
    )
  )

  val MOVIERATING = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL)
    )
  )

    val NUMBERSERIES = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL)
    )
  )

    val AGEANALYSIS = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL)
    )
  )

    val WORDCOUNT = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_OTHER)
    )
  )

    val EXTERNALCALL = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_OTHER)
    )
  )

    val FINDSALARY = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_NUMERICAL)
    )
  )

    val INSIDECIRCLE = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL)
    )
  )

    val MAPSTRING = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_OTHER)
    )
  )

  val RIGTEST = Array[Array[Schema[Any]]](
    // http://www.youtube.com,1000,100,100,920,2,advertisement
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_OTHER)
    )
  )

  val RIGTESTJOIN = Array[Array[Schema[Any]]](
    // http://www.youtube.com,1000,100,100,920,2,advertisement
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL)
    ),
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL)
    )
  )


  val INCOMEAGGREGATION = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL)
    )
  )

  val LOANTYPE = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_OTHER)
    )
  )

  val store_sales: Array[Schema[Any]] = Array(
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL)
  )

  val date_dim: Array[Schema[Any]] = Array(
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER)
  )

  val item: Array[Schema[Any]] = Array(
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_OTHER)
  )

  val store_returns: Array[Schema[Any]] = Array(
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL)
  )

  val store: Array[Schema[Any]] = Array(
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL)
  )

  val customer: Array[Schema[Any]] = Array(
   new Schema(Schema.TYPE_NUMERICAL),
   new Schema(Schema.TYPE_OTHER),
   new Schema(Schema.TYPE_NUMERICAL),
   new Schema(Schema.TYPE_NUMERICAL),
   new Schema(Schema.TYPE_NUMERICAL),
   new Schema(Schema.TYPE_NUMERICAL),
   new Schema(Schema.TYPE_NUMERICAL),
   new Schema(Schema.TYPE_OTHER),
   new Schema(Schema.TYPE_OTHER),
   new Schema(Schema.TYPE_OTHER),
   new Schema(Schema.TYPE_OTHER),
   new Schema(Schema.TYPE_NUMERICAL),
   new Schema(Schema.TYPE_NUMERICAL),
   new Schema(Schema.TYPE_NUMERICAL),
   new Schema(Schema.TYPE_OTHER),
   new Schema(Schema.TYPE_OTHER),
   new Schema(Schema.TYPE_OTHER),
   new Schema(Schema.TYPE_OTHER)
  )

  val customer_address: Array[Schema[Any]] = Array(
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_OTHER)
  )

  val customer_demographics: Array[Schema[Any]] = Array(
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL)
  )

  val promotion: Array[Schema[Any]] = Array(
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_OTHER)
  )

  val web_sales: Array[Schema[Any]] = Array(
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL)
  )

  val household_demographics: Array[Schema[Any]] = Array(
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_OTHER),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL)
  )


  val catalog_sales: Array[Schema[Any]] = Array(
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL),
    new Schema(Schema.TYPE_NUMERICAL)
  )

  val Q3 = Array[Array[Schema[Any]]](
    store_sales,
    date_dim,
    item
  )

  val Q1 = Array[Array[Schema[Any]]](
    store_returns,
    date_dim,
    store,
    customer
  )

  val Q6 = Array[Array[Schema[Any]]](
    customer_address,
    customer,
    store_sales,
    date_dim,
    item
  )

  val Q7 = Array[Array[Schema[Any]]](
    customer_demographics,
    promotion,
    store_sales,
    date_dim,
    item
  )

  val Q12 = Array[Array[Schema[Any]]](
    web_sales,
    date_dim,
    item
  )

  val Q13 = Array[Array[Schema[Any]]](
    store_sales,
    store,
    date_dim,
    household_demographics,
    customer_demographics,
    customer_address
  )

  val Q15 = Array[Array[Schema[Any]]](
    catalog_sales,
    customer,
    customer_address,
    date_dim
  )

  val Q19 = Array[Array[Schema[Any]]](
    date_dim,
    store_sales,
    item,
    customer,
    customer_address,
    store
  )

  val Q20 = Array[Array[Schema[Any]]](
    catalog_sales,
    date_dim,
    item
  )
}
