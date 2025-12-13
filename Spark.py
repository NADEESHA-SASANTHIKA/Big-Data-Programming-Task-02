val weatherDF = spark.read.option("header","true").csv("hdfs://namenode:9000/user/iitgcpuser/task02/input/weatherData.csv")
val locationDF = spark.read.option("header","true").csv("hdfs://namenode:9000/user/iitgcpuser/task02/input/locationData.csv")

weatherDF.show(5)
locationDF.show(5)

import org.apache.spark.sql.functions._
val df = weatherDF.withColumn("shortwave_radiation_sum", $"shortwave_radiation_sum".cast("double"))
                  .withColumn("date", to_date($"date", "M/d/yyyy"))
val df2 = df.withColumn("month", month($"date"))
            .withColumn("year", year($"date"))
val monthlyRadiation = df2.groupBy("year", "month")
  .agg(
    count("*").alias("total_days"),
    sum(when($"shortwave_radiation_sum" > 15, 1).otherwise(0)).alias("days_above_15")
  )
  .withColumn("percentage_above_15", ($"days_above_15" / $"total_days") * 100)

monthlyRadiation.show()
val monthlyTemp = df2.groupBy("year", "month")
  .agg(avg($"temperature_2m_max").alias("avg_max_temp"))
  .orderBy(desc("avg_max_temp"))

monthlyTemp.show(12)

val hottestMonthRow = monthlyTemp.first()
val hottestMonth = hottestMonthRow.getAs[Int]("month")
val hottestYear = hottestMonthRow.getAs[Int]("year")

val hottestMonthDF = df2.filter($"year" === hottestYear && $"month" === hottestMonth)

val hottestMonthWeekDF = hottestMonthDF.withColumn("week", weekofyear($"date"))
val weeklyMaxTemp = hottestMonthWeekDF.groupBy("week")
  .agg(max($"temperature_2m_max").alias("weekly_max_temp"))
  .orderBy("week")

weeklyMaxTemp.show()
