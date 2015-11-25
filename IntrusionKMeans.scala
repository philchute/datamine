//Updated 8/14/2015
//Total time: 666 s, 
//run sampling script to smaller dataset
//Link to download dataset http://kdd.ics.uci.edu/databases/kddcup99/kddcup99.html
//mv /home/jetzhong/Downloads/kddcup.data_10_percent.gz  .
//gunzip kddcup.data_10_percent.gz
//gunzip corrected.gz
//sbt package
//spark-submit \
//   --class "IntrusionKMeans" \
//   target/scala-2.10/scala-clustering-app_2.10-1.0.jar


import org.apache.spark.mllib.clustering._
//import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint}
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.log4j.{Level, Logger}

import net.liftweb.json.JsonDSL._
import net.liftweb.json.JsonAST._
import net.liftweb.json.Printer._
import net.liftweb.json.JObject
import net.liftweb.json._

import scala.util.Try
import org.apache.spark.{SparkConf, SparkContext}

//import spark.jobserver.{SparkJob,SparkJobValidation,SparkJobValid,SparkJobInvalid}
import com.typesafe.config.{Config, ConfigFactory}

import java.io.PrintWriter
import java.net.ServerSocket
//import java.text.{SimpleDateFormat, DateFormat}
//import java.util.Date
import scala.util.Random
//import org.apache.spark.streaming.{Seconds, StreamingContext}

object IntrusionKMeans{
   def main(args: Array[String]): Unit = {

   //this does not work for sbt run work for spark submit
   //Logger.getRootLogger.setLevel(Level.WARN)
   //change the log level
   AdjustLogLevel.setStreamingLogLevels()
   
   //This need to change to run in the cloud
   val sc = new SparkContext("local[2]", "Intrusion K-Means")
   //RDD[String]
   //I am using much smaller dataset
   val rawData = sc.textFile("/home/lance/BigData/FirstProject/kddcup.data_10_percent")
   
   //********###CODE###***************************************************//
   System.out.println("Total number of Sample \n" + rawData.count)

   // rawData.map(_.split(',').last).countByValue().toSeq.sortBy(_._2).
   //         reverse.foreach(println)
        
   
  //batch process
  //val Array(rawBatchData, testData) = rawData.randomSplit(Array(0.4, 0.6), seed = 11)
  val Array(rawBatchData, testData) = rawData.randomSplit(Array(0.01, 0.99), seed = 11)
  
  val batchLabelsAndDataVector = rawBatchData.map { line =>
	val buffer = line.split(',').toBuffer
	buffer.remove(1, 3)
	val label = buffer.remove(buffer.length - 1)
	val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
	(label, vector)
    }
    
 // batchLabelsAndDataVector.cache()

  //Get the values from the vector
  val batchDataVector = batchLabelsAndDataVector.values//batchLabelsAndDataVector.values.cache()

  //Create the normalize function on the vector
  val normalizeFunction = buildNormalizationFunction(batchDataVector)

  //Build th batch K means model
  val batchKMModel = buildBatchKMeansModel(batchDataVector, normalizeFunction)
   
   //Evaluate the clustering results using the distance score for Batch
   //Pass in normalized data
   println("Distance Score: " + calculateDistanceScore(batchDataVector, batchKMModel))
   
   //Evaluate the clustering results using entropy for Batch
   println("Entropy Score: " + EntropyScore(batchLabelsAndDataVector, batchKMModel))
   
   //Figure out what went into the clusters for Batch
   clusteringLabelMakeup(batchKMModel, batchLabelsAndDataVector)
   
   //Build the anomaly Detector
   val anomalyDetector = buildAnomalyDetector(batchKMModel, batchDataVector, normalizeFunction)
   
   //Start of the streaming k-means
 
   //Set up the streaming context and the stream
   val ssc = new StreamingContext(sc, Seconds(10))
   val stream = ssc.socketTextStream("localhost", 9999)
  
   // create a stream of labeled points
   val labeledDataStream = stream.map { line =>
   val buffer = line.split(',').toBuffer
   buffer.remove(1, 3)
   val label = buffer.remove(buffer.length - 1)
   val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
   (label, vector)
   }
   
   val dataStream = labeledDataStream.map { case (label, vector ) => vector }
    
   val k = 130 //number of cluster
   val weight = 100.0 // each cluster has equal weight
   val weightVector = Array.fill(k)(weight)

   val streamingKMmodel = new StreamingKMeans() 
     streamingKMmodel.setDecayFactor(1.0) 
     streamingKMmodel.setK(k) 
     streamingKMmodel.setInitialCenters(batchKMModel.clusterCenters, weightVector) 
     streamingKMmodel.trainOn(dataStream) 
     
     
    def costlyAveDisToCentroidScore1(data: RDD[Vector], newModel:KMeansModel): Double = 
    data.map(datum => distToCentroid(datum, newModel)).mean()
    
    //For each rdd, get the distance score and anomalies
    labeledDataStream.foreachRDD { (rdd, time) =>
      val dataRDD = rdd.map {case(lable, data) => data }
      val normalizedDataRDD = dataRDD.map(normalizeFunction)
      val latest = streamingKMmodel.latestModel()
      val distanceScore = costlyAveDisToCentroidScore1(normalizedDataRDD, latest)
      //get the anomalous data
      val anomalies = dataRDD.filter { datum => anomalyDetector(datum)}
    }
   
   //Save the clustering evaluations for d3 in json file
   
   ssc.start()
   ssc.awaitTermination()  
   sc.stop()

   }//END OF MAIN


   // Clustering, Take 0
   //this method contain some useful code to figure out what kind of data
   //is present in each cluster
   //myData only contain feature vector
   //myLableData contain both label and feature vector
   def clusteringTake0(myData: RDD[Vector], myLabelData:RDD[(String,Vector)]): Unit = {
      //The following code clusters the data to create a KMeansModel, and then prints  
      //its centroids. By default, K-means was fitting k = 2 clusters to the data.
      //kmeans: org.apache.spark.mllib.clustering.KMeans
      val kmeans = new KMeans()
      //model: org.apache.spark.mllib.clustering.KMeansModel
      val model = kmeans.run(myData)
      // res0: Array[org.apache.spark.mllib.linalg.Vector]
      model.clusterCenters.foreach(println)
      
      //This is a good opportunity to use the given labels to get an intuitive sense
      // of    what went into these two clusters, by counting the labels within each
      // cluster. The following code uses the model to assign each data point to a  
      //cluster, counts occurrences of cluster and label pairs, and prints them nicely
      
      //label is 23 distinct network attack type
      //clusterLabelCount: scala.collection.Map[(Int, String),Long] 
      val clusterLabelCount = myLabelData.map { case (label, datum) =>
          val cluster = model.predict(datum)
          (cluster, label)
      }.countByValue()
      clusterLabelCount.toSeq.sorted.foreach { 
         case ((cluster, label), count) => println(f"$cluster%1s$label%18s$count%8s")
      }

      
   }//end of clusteringTake0 method


   //distance: (a: org.apache.spark.mllib.linalg.Vector, 
   //b: org.apache.spark.mllib.linalg.Vector) Double
   //return Euclidean distance between two vectors, which is the double
   def distance(a: Vector, b: Vector) =
     math.sqrt(a.toArray.zip(b.toArray).
        map(p => p._1 - p._2).map(d => d * d).sum)

  //return distance between the data point and its nearest cluster's centroid
  //which is double
  def distToCentroid(datum: Vector, model: KMeansModel) = {
    //cluster: int  this is cluster to which a sample belongs to
    val cluster = model.predict(datum)
    val centroid = model.clusterCenters(cluster)
    distance(centroid, datum)
  }

  //define a function that measures the average distance to centroid for a 
  //model built with a given K
  def basicAveDisToCentroidScore(data: RDD[Vector], k: Int): Double = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    val model = kmeans.run(data)
    data.map(datum => distToCentroid(datum, model)).mean()
  }
  
  //set number of iteration and minimum convergence condition
  //average distance to the closest centroid as the clustering score
  def costlyAveDisToCentroidScore(data: RDD[Vector], k: Int): Double = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(data)
    data.map(datum => distToCentroid(datum, model)).mean()
}


  //this function will search best K based on average distance to centroid
  def searchBestKWithoutNormalizationUsingDistance(myData: RDD[Vector]): Unit = {
    //System.out.println("basicAveDisToCentroidScore")
    //(5 to 30 by 5).map(k => (k, basicAveDisToCentroidScore(myData, k))).
    //  foreach(println)

    //(30 to 100 by 10).par.map(k => (k, costlyAveDisToCentroid(myData, k))).
     // toList.foreach(println)
    System.out.println("costlyAveDisToCentroidScore")
    (30 to 100 by 10).map(k => (k, costlyAveDisToCentroidScore(myData, k))).
      toList.foreach(println)

  }

  /////////////////////////
  //FEATURE NORMALIZATION//
  /////////////////////////
  //this will  function. Given the input vector
  //this will produce the vector with each dimension normalized
  def buildNormalizationFunction(data: RDD[Vector]): (Vector => Vector) = {
    //RDD[Array[Double]]  Basically this is 2-D array
    val dataAsArray = data.map(_.toArray)
    //numCols: Int number of column
    val numCols = dataAsArray.first().length
    //n: Long   total number of samples
    val n = dataAsArray.count()
    //sums: Array[Double]
    //each element is total sum for one attribute or dimension
    val sums = dataAsArray.reduce(
        (a, b) => a.zip(b).map(t => t._1 + t._2))
    //sumSquares: Array[Double]  sum-of-square for each feature
    val sumSquares = dataAsArray.fold(
        new Array[Double](numCols)
    )(
        (a, b) => a.zip(b).map(t => t._1 + t._2 * t._2)
      )
    

    //stdevs: Array[Double] 
    //standard deviation for each attribute
    val stdevs = sumSquares.zip(sums).map {
        case (sumSq, sum) => math.sqrt(n * sumSq - sum * sum) / n
    }
    //means: Array[Double]
   //means for each attribute
   val means = sums.map(_ / n)
   //Define function: Given the input vector
   //this will produce the vector with each dimension normalized
   (datum: Vector) => {
       val normalizedArray = (datum.toArray, means, stdevs).zipped.map(
           (value, mean, stdev) =>
               if (stdev <= 0)  (value - mean) else  (value - mean) / stdev
       )
       Vectors.dense(normalizedArray)
   } 
 }//end of buildNormalizationFunction
 
 def costlyAveDisToCentroidScore(data: RDD[Vector], newModel:KMeansModel): Double = {
    data.map(datum => distToCentroid(datum, newModel)).mean()
}
 
  def searchBestKWithNormalizedDataUsingDistance(rawData: RDD[Vector]): Unit = {
    //normalizedData: RDD[org.apache.spark.mllib.linalg.Vector] 
    val normalizedData = rawData.map(buildNormalizationFunction(rawData)).cache()
   
    //(60 to 120 by 10).par.map(k =>
    //  (k, costlyAveDisToCentroid(normalizedData, k))).toList.foreach(println)
    (60 to 120 by 20).map{k => 
      System.out.println("K is " + k)
      (k, costlyAveDisToCentroidScore(normalizedData, k))}.toList.foreach(println)
    normalizedData.unpersist()
  }

  ///////////////////
  //Including CATEGORICAL VARIALBE//
  ///////////////////////
  //this function will include the categorical variable
  //this function will return the function. The returned function takes string of each
  // line and return label and unnormalized data vector for each line
  //buildCategoricalAndLabelFunction: (rawData: org.apache.spark.rdd.RDD[String])
  //    String => (String, org.apache.spark.mllib.linalg.Vector)
  def includeCategoricalFeatureAndLabel(rawData: RDD[String]): 
    (String => (String,Vector)) = {
    
    val splitData = rawData.map(_.split(','))
    //produce the mapping for each protocols
    // protocols: scala.collection.immutable.Map[String,Int] =
    //Map(tcp -> 0, icmp -> 1, udp -> 2)    
   // The zipWithIndex method returns a list of pairs where the second
   //component is the index of each element
    val protocols =  
        splitData.map(_(1)).distinct().collect().zipWithIndex.toMap
    val services =  
        splitData.map(_(2)).distinct().collect().zipWithIndex.toMap
    val tcpStates = 
        splitData.map(_(3)).distinct().collect().zipWithIndex.toMap
    

    (line: String) => {
        // line.split(',')   res0: Array[String]
        // buffer: scala.collection.mutable.Buffer[String]
        val buffer = line.split(',').toBuffer
        val protocol = buffer.remove(1)
        val service = buffer.remove(1)
        val tcpState = buffer.remove(1)
        val label = buffer.remove(buffer.length - 1)
        // vector: scala.collection.mutable.Buffer[Double]
        val vector = buffer.map(_.toDouble)
        val newProtocolFeatures = new Array[Double](protocols.size)
        newProtocolFeatures(protocols(protocol)) = 1.0
        val newServiceFeatures = new Array[Double](services.size)
        newServiceFeatures(services(service)) = 1.0
        val newTcpStateFeatures = new Array[Double](tcpStates.size)
        newTcpStateFeatures(tcpStates(tcpState)) = 1.0
        vector.insertAll(1, newTcpStateFeatures)
        vector.insertAll(1, newServiceFeatures)
        vector.insertAll(1, newProtocolFeatures)
        (label, Vectors.dense(vector.toArray))
    }
  }//end of includeCategoricalFeatureAndLabel function to include categoriacal feature

  //the input parameter is the raw data including each line of code
  def searchBestKWithNormalizedCategoricalUsingDistance(rawData: RDD[String]): Unit = {
    System.out.println("searchBestKWithNormalizedCategoricalUsingDistance")
    // parseFunction: String => (String, org.apache.spark.mllib.linalg.Vector)
    val parseFunction = includeCategoricalFeatureAndLabel(rawData)
    // data: org.apache.spark.rdd.RDD[org.apache.spark.mllib.linalg.Vector]
    //data only has value vectors without labels
    val data = rawData.map(parseFunction).values
    //data normalization
    val normalizedData =  
         data.map(buildNormalizationFunction(data)).cache()
    //(80 to 160 by 10).map(k =>
    //  (k, clusteringScore2(normalizedData, k))).toList.foreach(println)
    (30 to 160 by 10).map{k =>
         System.out.println("K is " + k)
        (k, costlyAveDisToCentroidScore(normalizedData, k))}.toList.foreach(println)
    normalizedData.unpersist()
  }//end of searchBestKWithNormalizedCategorical function

  ///////////////////////////////
  //Using Labels with Entropy//
  ///////////////////////////////
  //entropy: (counts: Iterable[Int])  Double
  def entropy(counts: Iterable[Int]) = {
    val values = counts.filter(_ > 0)
    val n: Double = values.sum
    values.map { v =>
        val p = v / n
        -p * math.log(p)
    }.sum
  }//end of entropy function
  
  //weighted average of entropy can be used as a cluster score
  //input of this function will RDD. Each element of RDD is label and normalized data
  //return Double value
  def costlyWeightedAveEntropyScore(normalizedLabelsAndData: RDD[(String,Vector)], 
                                                                   k: Int) = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(normalizedLabelsAndData.values)
    // Predict cluster for each datum
    // rdd.RDD[(String, Int)]  each element is the label and cluster num
    //  it belongs to
    val labelsAndClusters = normalizedLabelsAndData.mapValues(model.predict)
    // Swap keys / values
    //rdd.RDD[(Int, String)]
    //lable is 23 type of attacks or normal
    val clustersAndLabels = labelsAndClusters.map(_.swap)
    // Extract collections of labels, per cluster
    // Key is cluster ID
    // rdd.RDD[Iterable[String]]
    //each element is all labels for one cluster
    val labelsInCluster = clustersAndLabels.groupByKey().values
    // Count labels in collections
    //RDD[scala.collection.immutable.Iterable[Int]]
    //each element is total count of each label for each cluster
    val labelCounts = labelsInCluster.map(_.groupBy(l => l).map(_._2.size))
    //n: Long = 494021
    // total sample size
    val n = normalizedLabelsAndData.count()
    // Average entropy weighted by cluster size
    //m is total count for each label for one cluster
    //entropy(m) calcuate entropy for one cluster
    labelCounts.map(m => m.sum * entropy(m)).sum / n
  }//end of costlyWeightedAveEntropyScore

  //use weighted average entropy as the clustering score
  //vector include the categorical features.
  //first parse the function into label and data vector format for each line
  //then normalize data vector
  def searchBestKWithUsingEntropy(rawData: RDD[String]): Unit = {
    System.out.println("searchBestKWithUsingEntropy")
    // parseFunction: String => (String, org.apache.spark.mllib.linalg.Vector)
    val parseFunction = includeCategoricalFeatureAndLabel(rawData)
    // labelsAndData: 
    //org.apache.spark.rdd.RDD[(String, org.apache.spark.mllib.linalg.Vector)]
    val labelsAndData = rawData.map(parseFunction)
    // normalizedLabelsAndData: 
    //org.apache.spark.rdd.RDD[(String, org.apache.spark.mllib.linalg.Vector)
   val normalizedLabelsAndData = labelsAndData.
     mapValues(buildNormalizationFunction(labelsAndData.values)).cache()
    //(80 to 160 by 10).map(k =>
    //  (k, clusteringScore3(normalizedLabelsAndData, k))).
    //       toList.foreach(println)
    (30 to 160 by 10).map{k =>
     System.out.println("K is " + k)
     (k, costlyWeightedAveEntropyScore(normalizedLabelsAndData, k))}.toList.
           foreach(println)
    normalizedLabelsAndData.unpersist()
  }//end of searchBestKWithUsingEntropy function

  /////////////////////////////////////////////////////////
  ///clustering in Action. Detect Anomalous samples////
  ////////////////////////////////////////////////////////
  // Detect anomalies
  //input of RDD[Vector], which only contains data without
  //label
  //normalizeFunction: given vector, produce normalize entry for vector
  //this will return a function.  Given vector, this function will tell
  //whether the given data is anomalous or not.
  def buildAnomalyDetector1(
     data: RDD[Vector],
     normalizeFunction: (Vector => Vector)): (Vector => Boolean) = {
    val normalizedData = data.map(normalizeFunction)
    normalizedData.cache()
    val kmeans = new KMeans()
    kmeans.setK(150)
    //kmeans.setK(20)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(normalizedData)
    normalizedData.unpersist()

    //RDD[Double]
    //each element is distance to the closest centroid for all data
    val distances = normalizedData.
        map(datum => distToCentroid(datum, model))
    //pick 100th farthest data point from among known data
    //top function from RDD
    //def top(num: Int)(implicit ord: Ordering[T]): Array[T]
    //Returns the top k (largest) elements from this RDD as defined by the specified 
     //implicit Ordering[T]. This does the opposite of takeOrdered.
    val threshold = distances.top(100).last
    (datum: Vector) => distToCentroid(normalizeFunction(datum), model) > threshold
  }//end of buildAnomalyDetector function
  
  /////////////////////////////////////////////////////////
///clustering in Action. Detect Anomalous samples////
////////////////////////////////////////////////////////
// Detect anomalies
//input of RDD[Vector], which only contains data without label
//normalizeFunction: given vector, produce normalize entry for vector
//this will return a function.  Given vector, this function will tell
//whether the given data is anomalous or not.
def buildAnomalyDetector(
     model: KMeansModel,
     data: RDD[Vector],
     normalizeFunction: (Vector => Vector)): (Vector => Boolean) = {
    val normalizedData = data.map(normalizeFunction)
     //RDD[Double]
    //each element is distance to the closest centroid for all data
    val distances = normalizedData.map(datum => distToCentroid(datum, model))
    //pick 100th farthest data point from among known data
    //top function from RDD
    //def top(num: Int)(implicit ord: Ordering[T]): Array[T]
    //Returns the top k (largest) elements from this RDD as defined by the specified 
     //implicit Ordering[T]. This does the opposite of takeOrdered.
    val threshold = distances.top(100).last
    (datum: Vector) => distToCentroid(normalizeFunction(datum), model) > threshold
  }//end of buildAnomalyDetector function
  
  //this function will print out the anomoulous data
  def anomalies(rawData: RDD[String]) : Unit = {
    System.out.println("anomalies")
    // parseFunction: String => (String, org.apache.spark.mllib.linalg.Vector)
     val parseFunction = includeCategoricalFeatureAndLabel(rawData)
    //in order to interpret the results, we keep the original line of input
    //with the parsed feature vector
    // originalAndData: RDD[(String, org.apache.spark.mllib.linalg.Vector)]
    //each line is the key and value is data vector
    val originalAndData = rawData.map(line => (line, parseFunction(line)._2))
    val data = originalAndData.values
    val normalizeFunction = buildNormalizationFunction(data)
    val anomalyDetector = buildAnomalyDetector1(data, normalizeFunction)
    


   // anomalies: org.apache.spark.rdd.RDD[String]
   val anomalies = originalAndData.filter {
        case (original, datum) => anomalyDetector(datum)
    }.keys
    //anomalies.take(10).foreach(println)
    anomalies.foreach(println)
    //print label only
    anomalies.map(_.split(',').last).foreach(println)
    System.out.println("Total Anomalous Samples: " + anomalies.count)
  }//end of anomalies function


  //Build batch K-means model based on batch dataset 
  def buildBatchKMeansModel(
  data: RDD[Vector],
  normalizeFunction:(Vector => Vector)): KMeansModel = {
  val normalizedData = data.map(normalizeFunction)
  normalizedData.cache()
  val kmeans = new KMeans()
  kmeans.setK(130) //number of cluster
  kmeans.setRuns(10)
  kmeans.setEpsilon(1.0e-6)
  val model = kmeans.run(normalizedData)
  normalizedData.unpersist()
  model
  }//End of buildBatchKMeansModel function
  
  //Print out what went into the clusters
  def clusteringLabelMakeup(model: KMeansModel,
  myLabelData:RDD[(String,Vector)]): Unit = {
  val clusterLabelCount = myLabelData.map{ case (label, datum) =>
  val cluster = model.predict(datum)
  (cluster,label)
  }.countByValue()
  clusterLabelCount.toSeq.sorted.foreach{
  case ((cluster, label), count) => println(f"$cluster%1s$label%18s$count%8s")
  //Transform the map into something that will work for json
  //val output = clusterLabelCount.map{
  //case ((cluster, label), count) => ("cluster" -> cluster) ~ ("label" -> 
  //label.stripSuffix(".")) ~ ("count" -> count)
  //}
  //println(JsonAST.compactRender(output).stripSuffix("."))
  //JsonAST.compactRender(output).stripSuffix(".")
  }}//End of clusteringLabelMakeup Function
  
  //Distance score to evaluate the cluster
  //data is normalized vector
  def calculateDistanceScore(data: RDD[Vector], 
  model: KMeansModel): Double = {
  data.map(datum => 
  distToCentroid(datum, model)).mean()
  }
  
  //input of this function will RDD. Each element of RDD is label and normalized data
  def EntropyScore(normalizedLabelsAndData: RDD[(String,Vector)], model: KMeansModel) = {
  val labelsAndClusters = normalizedLabelsAndData.mapValues(model.predict)
  val clustersAndLabels = labelsAndClusters.map(_.swap)
  val labelsInCluster = clustersAndLabels.groupByKey().values
  val labelCounts = labelsInCluster.map(_.groupBy(l => l).map(_._2.size))
  val n = normalizedLabelsAndData.count()
  labelCounts.map(m => m.sum * entropy(m)).sum / n
  }
  
}//end of object
