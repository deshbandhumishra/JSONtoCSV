import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Main extends App {
  var file: String = "resource/add.json"

  /*"addresses": [
  {
    "adrCityNm": "BEND",
    "adrStrLine1Txt": "1234 INTERNATIONAL STREET",
    "cntryCd": "USA",
    "geoStCd": "NV",
    "pk": {
      "adrTypeCd": "M",
      "id": "100000979"
    },
    "pstlCd": "48853"
  },*/

  val pk_addres_schema =StructType(
    List(
      StructField("adrTypeCd", StringType, nullable = true),
      StructField("id", StringType, true)))


  val  addresses_schema =StructType(
    List(
      StructField("cntctPhnNum", StringType,true),
      StructField("adrStrLine1Txt", StringType,true),
      StructField("cntryCd", StringType,true),
      StructField("geoStCd", StringType,true),
      StructField("pk", pk_addres_schema,true),
      StructField("pstlCd", StringType,true)
    ))
  /*"cntctPhnNum": "1234567890",
  "firstName": "Johny",
  "lastName": "Hilton",
  "lastUpdtdDt": "05/13/2020",
  "pk": {
    "id": "100000979"
  },
  */


  val pk_customer_schema = StructType(
    List(
      StructField("id", StringType, true)))

  val customer_schema = new StructType()
    .add("cntctPhnNum", StringType,true)
    .add("firstName", StringType,true)
    .add("lastName", StringType,true)
    .add("lastUpdtdDt", StringType,true)
    .add("pk", pk_customer_schema,true)
    .add("addresses", addresses_schema,true)

  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("JSONtoCSV").getOrCreate()
  import spark.implicits._
  val customer_df = spark.read.option("multiline","true").option("mode","PERMISSIVE").json(file) //
  //val addresses_df = spark.read.option("multiline","true").schema(addresses_schema).json(file) //

  customer_df.show()
  customer_df.printSchema()
  //addresses_df.show()

  val customer_df_final = customer_df.select(
    $"cntctPhnNum".as("cntctPhnNum"),
    $"firstName".as("firstName"),
    $"lastName".as("lastName"),
    $"lastUpdtdDt".as("lastUpdtdDt"),
    $"pk".getField("id").as("customer.d"))

  val addresses_df_final = customer_df.withColumn("addresses",explode($"addresses")).select("addresses.adrCityNm",
    "addresses.adrStrLine1Txt","addresses.cntryCd","addresses.geoStCd","addresses.pk.adrTypeCd","addresses.pk.id","addresses.pstlCd")


  customer_df_final.printSchema()
  customer_df_final.show()
  addresses_df_final.show()
  //  val address_df_final = addresses_df.select(

  //address_df_final.printSchema()
  //address_df_final.show(2)
  val finaltable = customer_df_final.crossJoin(addresses_df_final)
  finaltable.show(5)
  finaltable.write.format("csv").option("header","True").mode("overwrite").option("sep",",").save("resource/out1/")
}