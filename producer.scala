
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



//Producer
object producer{
        def main(args: Array[String]){
                AdjustLogLevel.setStreamingLogLevels()
                val conf = new SparkConf().setMaster("local").setAppName("producer")
                val sc = new SparkContext(conf)
                //Create random number generator
                val random = new Random()
                //Maximum number of events per second
                val MaxEvents = 100
                //create object of data
                val lines = sc.textFile("/home/lance/BigData/FirstProject/kddcup.data_10_percent")
                //Split the data
		val Array(streamingData, testData) = lines.randomSplit(Array(0.4,0.6), seed = 11)
                def generateNetworkEvents(n:Int) = {
                  (1 to n).map{i =>
                   val line = streamingData.takeSample(true, 1, random.nextLong)
                  (line)
                  }
                }  
                //create a network producer
                val listener = new ServerSocket(9999)
                println("Listening on port: 9999")
                while(true) {
                        //create a new network socket
                        val socket = listener.accept()
                        new Thread() {
                                override def run = {
                                        println("Client connected from: "+ socket.getInetAddress)
                                        val out = new PrintWriter(socket.getOutputStream(), true)
                                        while(true) {
                                                Thread.sleep(1000)
                                                val num = random.nextInt(MaxEvents)
                                                val networkEvents = generateNetworkEvents(num)
                                                networkEvents.foreach { event =>
                                                        //productIterator convert tuple into iterator
                                                        //out.write(event.productIterator.mkString(","))
                                                        out.write(event.mkString(","))
                                                        out.write("\n")
                                                }
                                                out.flush()
                                                println(s"Created $num events...")
                                        }//end while
                                        socket.close()
                                }
                        }.start()
                }//end while
        }
}