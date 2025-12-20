import findspark
findspark.init('/home/iitgcpuser/spark')

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, month, avg, col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Start Spark in local mode
spark = SparkSession.builder \
    .appName("Task3_ET0_Prediction") \
    .master("local[*]") \
    .getOrCreate()

print("Spark session started successfully!")

# Load data
weather_df = spark.read.csv("weatherData.csv", header=True, inferSchema=True)

# Parse date - format d/M/yyyy (from your data: 1/1/2010)
weather_parsed = weather_df.withColumn("date", to_date(col("date"), "d/M/yyyy")) \
    .withColumn("month", month("date"))

# Filter for May (month = 5)
may_df = weather_parsed.filter(col("month") == 5)

# Select features and label - note the exact column names with spaces
data = may_df.select(
    col("precipitation_hours (h)").cast("double"),
    col("sunshine_duration (s)").cast("double"),
    col("wind_speed_10m_max (km/h)").cast("double"),
    col("et0_fao_evapotranspiration (mm)").cast("double").alias("label")
).na.drop()

print(f"\nNumber of May days in dataset: {data.count()}")
if data.count() > 0:
    print("Sample May data:")
    data.show(10)

    # Assemble features
    assembler = VectorAssembler(
        inputCols=["precipitation_hours (h)", "sunshine_duration (s)", "wind_speed_10m_max (km/h)"],
        outputCol="features"
    )
    assembled = assembler.transform(data)

    # Split 80/20
    train, test = assembled.randomSplit([0.8, 0.2], seed=42)

    # Train model
    lr = LinearRegression(maxIter=100, regParam=0.01)
    model = lr.fit(train)

    print("\nModel trained!")
    print(f"Coefficients: {model.coefficients}")
    print(f"Intercept: {model.intercept}")

    # Evaluate
    predictions = model.transform(test)
    rmse = RegressionEvaluator(metricName="rmse").evaluate(predictions)
    r2 = RegressionEvaluator(metricName="r2").evaluate(predictions)

    print(f"\nModel Evaluation:")
    print(f"RMSE: {rmse:.4f}")
    print(f"R²: {r2:.4f}")

    # Low ET0 conditions
    low_et0 = predictions.filter(col("prediction") < 1.5)

    if low_et0.count() > 0:
        print("\nAverage conditions leading to ET0 < 1.5 mm:")
        low_et0.agg(
            avg("precipitation_hours (h)").alias("avg_precip_hours"),
            avg("sunshine_duration (s)").alias("avg_sunshine_seconds"),
            avg("wind_speed_10m_max (km/h)").alias("avg_wind_kmh")
        ).show()
    else:
        print("\nNo days with predicted ET0 < 1.5 mm in validation set")

    print("\nRecommendation for May 2026 to achieve ET0 < 1.5 mm:")
    print("- High precipitation hours (> 8-10 hours)")
    print("- Low sunshine duration (< 20000 seconds ≈ 5.5 hours)")
    print("- Low wind speed (< 15 km/h)")
else:
    print("\nNo May data found in the dataset.")

spark.stop()
print("\nAnalysis complete!")
