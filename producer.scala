import java.io.PrintWriter
import java.net.ServerSocket
//import java.text.{SimpleDateFormat, DateFormat}
//import java.util.Date
import scala.util.Random
import org.apache.spark.SparkContext
//import org.apache.spark.streaming.{Seconds, StreamingContext}

object producer{
        def main(args: Array[String]){
                //Create random number generator
                val random = new Random()
                //Maximum number of events per second
                val MaxEvents = 100
                //create object of data
                val lines = sc.textFile("/kddcup.data_10_percent")
                //val linesOfValues = scala.io.Source.fromInputStream(namesResource).getLines().toList.head.split("\n").toSeq

                def generateNetworkEvents(n:Int) = {
                   val line = lines.takeSample(true, 1, random.nextLong)
                   val duration = line.next()
                   val protocol_type = line.next()
                   val service = line.next()
                   val flag = line.next()
                   val src_bytes = line.next()
                   val dst_bytes = line.nextInt()
                   val land = line.next()
                   val wrong_fragment = line.nextInt()
                   val urgent = line.nextInt()
                   val hot = line.nextInt()
                   val num_failed_logins = line.nextInt()
                   val logged_in = line.next()
                   val num_compromised = line.nextInt()
                   val root_shell = line.nextInt()
                   val su_attempted = line.nextInt()
                   val num_root = line.nextInt()
                   val num_file_creations = line.nextInt()
                   val num_shells = line.nextInt()
                   val num_access_files = line.nextInt()
                   val num_outbound_cmds = line.nextInt()
                   val is_host_login = line.next()
                   val is_guest_login = line.next()
                   val count = line.nextInt()
                   val srv_count = line.nextInt()
                   val serror_rate = line.nextInt()
                   val srv_serror_rate = line.nextInt()
                   val rerror_rate = line.nextInt()
                   val srv_rerror_rate = line.nextInt()
                   val same_srv_rate = line.nextInt()
                   val diff_srv_rate = line.nextInt()
                   val srv_diff_host_rate = line.nextInt()
                   val dst_host_count = line.nextInt()
                   val dst_host_srv_count = line.nextInt()
                   val dst_host_same_srv_rate = line.nextInt()
                   val dst_host_diff_srv_rate = line.nextInt()
                   val dst_host_same_src_port_rate = line.nextInt()
                   val dst_host_srv_diff_host_rate = line.nextInt()
                   val dst_host_serror_rate = line.nextInt()
                   val dst_host_srv_serror_rate = line.nextInt()
                   val dst_host_rerror_rate = line.nextInt()
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
                                                        out.write(event.productIterator.mkString(","))
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
