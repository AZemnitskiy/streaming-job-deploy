import java.net.ConnectException
import com.persist.JsonOps.{Json, jget}
import scalaj.http.{Http, HttpResponse}

class HttpRequestTopic( ipTopics: String, portTopicsKafkaManager: Int, portTopics: Int) {

  def httpGetTopicsString():String= {
    val url=  s"http://${ipTopics}:${portTopics}/topics/"
    try{
      io.Source.fromURL(url).mkString
    }catch{
      case e: ConnectException => throw new Exception(s"Cannot connect to ${url}. Exception message: ${'"'}${e.getMessage}${'"'}")
    }
  }

  def httpDeletetTopic( clusterName:String, topicName: String ): HttpResponse[String] = {
    val url = s"http://${ipTopics}:${portTopicsKafkaManager.toString}/clusters/${clusterName}/topics/delete?t=${topicName}"
    try {
      val response = Http(url)
       .postForm
       .param("topic", topicName)
       .asString

      if (response.code == 200) {
       println(s"SUCCESS: Delete topic ${'"'}${topicName}${'"'} ")
      } else {
       val parseJson = Json(response.body)
       val message = jget(parseJson, "message")
       println(s"FAILURE: Cannot delete topic ${'"'}${topicName}${'"'} : ${message}")
      }
      response
   }catch {
     case e: ConnectException => throw new Exception(s"Cannot connect to ${url}. Exception message: ${'"'}${e.getMessage}${'"'}")
   }
  }

  def httpCreateTopicsOnKafkaAndCheckClusterExist( zkHosts: String, kafkaVersion: String, clusterName: String, topic: String, partitions: Int, replication: Int) : HttpResponse[String] ={
    var response= httpCreateTopicsOnKafka(ipTopics, portTopicsKafkaManager, clusterName, topic, partitions, replication)

    if (response.toString.contains("Unknown cluster")) {
      println(s"Cluster ${clusterName} is not existing. We just created it")
      //Create Cluster
      val responseCluster = Http(s"http://${ipTopics}:${portTopicsKafkaManager}/clusters")
        .postForm
        .param("name",clusterName)
        .param("zkHosts",zkHosts)//"zookeeper:2181"
        .param("kafkaVersion",kafkaVersion)//"0.9.0.1"
        .asString

      //then post (create topic)
      response = httpCreateTopicsOnKafka(ipTopics, portTopicsKafkaManager, clusterName, topic, partitions, replication)
    }

    if(response.code == 200)
    {
      println(s"SUCCESS: Creation on Kafka of topic ${'"'}${topic}${'"'}" )
    }else{
      val parseJson =Json(response.body)
      val message = jget(parseJson, "message")
      println(s"FAILURE: Cannot create topic on Kafka ${'"'}${topic}${'"'} : ${message}")
    }
    response
  }


  def httpCreateTopicsOnKafka(ipTopics: String, portTopicsKafkaManager: Int, clusterName: String, topic: String, partitions: Int, replication: Int) : HttpResponse[String] = {
    val response = Http(s"http://${ipTopics}:${portTopicsKafkaManager}/clusters/${clusterName}/topics/create")
      .postForm
      .param("partitions", partitions.toString)
      .param("replication", replication.toString)
      .param("topic", topic)
      .asString

    response
  }

}
