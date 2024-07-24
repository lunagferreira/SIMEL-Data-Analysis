```scala
// Databricks notebook source

// DBTITLE 1,Libraries
// Importing the required libraries
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.SparkConf

import java.util.UUID
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZoneId}

// Initializing Spark session
val spark = SparkSession.builder.appName("PracticalCase2").getOrCreate()

// DBTITLE 1,Schema and Reading
// Defining the schema
def simelSchema: StructType = {
  StructType(
    Array(
      StructField("raw_cups", StringType, nullable = true),
      StructField("anio", StringType, nullable = true),
      StructField("mes", StringType, nullable = true),
      StructField("dia", StringType, nullable = true),
      StructField("hora", IntegerType, nullable = true),
      StructField("magnitud", StringType, nullable = true),
      StructField("valor_energia", DecimalType(10, 3), nullable = true),
      StructField("firmeza", StringType, nullable = true),
      StructField("cierre", StringType, nullable = true),
      StructField("tipo_medida", StringType, nullable = true)
    )
  )
}

// Reading files from the DBFS directory
val DBFS_TEMP = "/FileStore/PracticalCase2Files/"

// Define UDF to extract file name
def get_filename = udf((path: String) => {
  val parts = path.split("/")
  parts(parts.length - 1)
})

val df = spark.read
  .option("sep", ";")
  .option("header", false)
  .schema(simelSchema)
  .csv(DBFS_TEMP + "*")
  .withColumn("file", get_filename(input_file_name()))
  .distinct()

// Displaying DF
df.show(5, false)

// DBTITLE 1,UUID and CUPS
// Function to generate UUID
val uuidUDF = udf(() => UUID.randomUUID().toString)

// Adding UUID column
val dfWithUUID = df.withColumn("uuid", uuidUDF())

// Extracting CUPS from raw_cups
val extractCupsUDF = udf((raw_cups: String) => raw_cups.substring(0, 20)) // Taking first 20 characters

val dfWithCups = dfWithUUID.withColumn("cups", extractCupsUDF(col("raw_cups")))

dfWithCups.show(10, 50, true)

// DBTITLE 1,F_processo
// Formating the current date as yyyymmdd
val fProceso = LocalDate.now.format(DateTimeFormatter.BASIC_ISO_DATE)

// Adding the f_proceso column
val dfWithFProceso = dfWithCups.withColumn("f_proceso", lit(fProceso))

dfWithFProceso.show(10, 50, true)

// DBTITLE 1,F_despligue
// Adding the f_despliegue column 
val dfWithFDespliegue = dfWithFProceso.withColumn("f_despliegue", lit(null))

// DBTITLE 1,Date
// Combining anio, mes, and dia into a single date column
val dfWithDate: DataFrame = dfWithFDespliegue.withColumn("date", concat_ws("/", col("anio"), col("mes"), col("dia"))) 

dfWithDate.show(10, 50, true)

// DBTITLE 1,Estacion
// UDF to determine the season based on the date and hour
// def udfEstacion(date: String, hour: Int): Int = {
//   println(s"Received date: $date and hour: $hour")

//   val zoneId = ZoneId.of("Europe/Madrid")

//   val formatter = DateTimeFormatter.ofPattern("yyyy/MM/ddHHmm")

//   val dateTimeStr = date + "0000" // Appending hour to date and parse it (yyyyMMdd0000)
//   println(s"dateTimeStr: $dateTimeStr")
  
//   val localDateTime = LocalDateTime.parse(dateTimeStr, formatter).plusHours(hour)
//   println(s"LocalDateTime: $localDateTime")
  
//   val zonedDateTime = localDateTime.atZone(zoneId)
//   println(s"ZonedDateTime: $zonedDateTime")
  
//   val instant = zonedDateTime.toInstant
//   println(s"instant: $instant")

//   if (zoneId.getRules.isDaylightSavings(instant)) 1 else 0
// }
def udfEstacion(fecha: String, hora: Int): Int = {
    val zoneId = ZoneId.of("Europe/Madrid")
    val formatter = DateTimeFormatter.ofPattern("yyyy/MM/ddHHmm")
    val i = LocalDateTime.parse(fecha + "0000", formatter).plusHours(hora).atZone(zoneId).toInstant

    if (zoneId.getRules.isDaylightSavings(i)) 1 else 0
  }

// def udf_estacion(): UserDefinedFunction = {
//   udf[Int, String, Int]((date, hour) => udfEstacion(date, hour))
// }

def udf_estacion(): UserDefinedFunction = {
    udf[Int, String, Int]((fecha, hora) => udfEstacion(fecha, hora))
  }

// Adding columns for processing dates and hours
val dfWithDates: DataFrame = dfWithDate
  .withColumn("hours_per_day", count("hora").over(Window.partitionBy("raw_cups", "magnitud", "file"))) // Counting hours per day
  .withColumn("corrected_hour", when(col("hours_per_day") === 23 && col("hora") <= 2, col("hora") - 1) // Correcting the hour based on the number of hours per day 
    .when(col("hours_per_day") === 23 && col("hora") > 2, col("hora"))
    .when(col("hours_per_day") === 25 && col("hora") <= 3, col("hora") - 1)
    .when(col("hours_per_day") === 25 && col("hora") > 3, col("hora") - 2)
    .otherwise(col("hora") - 1))

// Adding the initial season column using udf_estacion based on the formatted date
val dfWithInitialEstacion: DataFrame = dfWithDates.withColumn("default_season", udf_estacion()(col("date"), lit(0)))

// Adding the final season column based on the corrected hour and hours per day
val dfWithEstacion: DataFrame = dfWithInitialEstacion.withColumn("estacion", when(col("hours_per_day") === 23 && col("hora") <= 2, col("default_season"))
  .when(col("hours_per_day") === 23 && col("hora") > 2, lit(1))
  .when(col("hours_per_day") === 25 && col("hora") <= 3, col("default_season"))
  .when(col("hours_per_day") === 25 && col("hora") > 3, lit(0))
  .otherwise(col("default_season")))

// Dropping temporary columns and renaming corrected_hour to hour
val dfWithFinalEstacion: DataFrame = dfWithEstacion.drop("default_season", "hours_per_day", "date", "hora").withColumnRenamed("corrected_hour", "hora")

// DBTITLE 1,Final DF
// Selecting and renaming columns to match the final schema
val finalDF = dfWithFDespliegue.select(
  "uuid",
  "raw_cups",
  "cups",
  "anio",
  "mes",
  "dia",
  "hora",
  // "estacion",
  "magnitud",
  "valor_energia",
  "firmeza",
  "cierre",
  "tipo_medida",
  "f_proceso",
  "file",
  "f_despliegue"
)

// Displaying the final dataframe
finalDF.show(10, 100, true)
