
//$SPARK_HOME/bin/spark-shell 

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

val data = sc.textFile("WDATA_8MOS_CLEAN.csv") //WDATA_NEW_FORM.csv WDATA_NEW_FORM_CLEAN_TEMP.csv
val header = data.first
val rows = data.filter(l => l != header)

//case class CC1(avg_ATOA: Integer, avg_ATODPT: Integer, avg_SCOCHD: Integer, avg_APOSLP: Integer, avg_VODD: Integer, avg_WODA: Integer, avg_WOSR: Integer, YEAR_GEO_LOC: String, YEAR: Integer, Latitude: Integer, Longitude: Integer)
case class CC2(year_month_lat_long: String, avg_temp: Int, c_year: String, c_month: String, c_lat: String, c_long: String, year: Int, month: Int, lat: Int, long: Int)

val allSplit = rows.map(line => line.split(","))

//val allData = allSplit.map( p => CC1( p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toInt, p(5).trim.toInt, p(6).trim.toInt, p(7).toString, p(8).trim.toInt, p(9).trim.toInt, p(10).trim.toInt))
val allData = allSplit.map( p => CC2( p(0).trim.toString, p(1).trim.toString, p(2).trim.toString, p(3).trim.toString, p(4).trim.toString, p(5).trim.toString, p(6).trim.toString, p(7).toString, p(8).trim.toString, p(9).trim.toString, p(10).trim.toString))

val allDF = allData.toDF()

allDF.registerTempTable("weather")

val weather = sqlContext.sql("SELECT avg_ATOA,YEAR,LATITUDE,LONGITUDE FROM weather WHERE avg_ATOA IS NOT NULL")
val train_data = weather.rdd.map(r => Vectors.dense( r.getInt(0), r.getInt(8), r.getInt(9), r.getInt(10)))

//val rowsRDD = allDF.rdd.map(r => (r.getInt(0), r.getInt(1), r.getInt(2), r.getInt(3), r.getInt(4), r.getInt(5), r.getInt(6), r.getString(7), r.getInt(8), r.getInt(9), r.getInt(10)))

//rowsRDD.cache()

//val vectors = allDF.rdd.map(r => Vectors.dense( r.getInt(0), r.getInt(8), r.getInt(9), r.getInt(10)))

//vectors.cache()


val kMeansModel = KMeans.train(train_data, 2, 20)

kMeansModel.clusterCenters.foreach(println)

val predictions = rowsRDD.map{r => (r._6, kMeansModel.predict(Vectors.dense(r._7, r._8, r._9) ))}

val predDF = predictions.toDF(“ID”, “CLUSTER”)

val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()
// Cluster the data into two classes using KMeans
val numClusters = 2
val numIterations = 20
val clusters = KMeans.train(parsedData, numClusters, numIterations)
val clusters = KMeans.train(parsedData, 2, 20)

// Evaluate clustering by computing Within Set Sum of Squared Errors
val WSSSE = clusters.computeCost(parsedData)
println("Within Set Sum of Squared Errors = " + WSSSE)