import java.io.PrintWriter
import java.net.ServerSocket
//import java.text.{SimpleDateFormat, DateFormat}
//import java.util.Date
import scala.util.Random
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
//import org.apache.spark.streaming.{Seconds, StreamingContext}

object producer{
        def main(args: Array[String]){
                val conf = new SparkConf().setMaster("local").setAppName("producer")
                val sc = new SparkContext(conf)
                //Create random number generator
                val random = new Random()
                //Maximum number of events per second
                val MaxEvents = 100
                //create object of data
                val lines = sc.textFile("/kddcup.data_10_percent")
                def generateNetworkEvents(n:Int) = {
                  (1 to n).map{i =>
                   val line = lines.takeSample(true, 1, random.nextLong)
                   /*val duration = line(0)
                   val protocol_type = line(1)
                   val service = line(2)
                   val flag = line(3)
                   val src_bytes = line(4)
                   val dst_bytes = line(5)
                   val land = line(6)
                   val wrong_fragment = line(7)
                   val urgent = line(8)
                   val hot = line(9)
                   val num_failed_logins = line(10)
                   val logged_in = line(11)
                   val num_compromised = line(12)
                   val root_shell = line(13)
                   val su_attempted = line(14)
                   val num_root = line(15)
                   val num_file_creations = line(16)
                   val num_shells = line(17)
                   val num_access_files = line(18)
                   val num_outbound_cmds = line(19)
                   val is_host_login = line(20)
                   val is_guest_login = line(21)
                   val count = line(22)
                   val srv_count = line(23)
                   val serror_rate = line(24)
                   val srv_serror_rate = line(25)
                   val rerror_rate = line(26)
                   val srv_rerror_rate = line(27)
                   val same_srv_rate = line(28)
                   val diff_srv_rate = line(29)
                   val srv_diff_host_rate = line(30)
                   val dst_host_count = line(31)
                   val dst_host_srv_count = line(32)
                   val dst_host_same_srv_rate = line(33)
                   val dst_host_diff_srv_rate = line(34)
                   val dst_host_same_src_port_rate = line(35)
                   val dst_host_srv_diff_host_rate = line(36)
                   val dst_host_serror_rate = line(37)
                   val dst_host_srv_serror_rate = line(38)
                   val dst_host_rerror_rate = line(39)
                   (duration, protocol_type, service, flag, src_bytes, dst_bytes, land, wrong_fragment, 
                        urgent, hot, num_failed_logins, logged_in, num_compromised, root_shell, su_attempted, 
                        num_root, num_file_creations, num_shells, num_access_files, num_outbound_cmds, is_host_login, 
                        is_guest_login, count, srv_count, serror_rate, srv_serror_rate, rerror_rate, srv_rerror_rate, 
                        same_srv_rate, diff_srv_rate, srv_diff_host_rate, dst_host_count, dst_host_srv_count, 
                        dst_host_same_srv_rate, dst_host_diff_srv_rate, dst_host_same_src_port_rate, dst_host_srv_diff_host_rate, 
                        dst_host_serror_rate, dst_host_srv_serror_rate, dst_host_rerror_rate, dst_host_srv_rerror_rate)
                  */
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
