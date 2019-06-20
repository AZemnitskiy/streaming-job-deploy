import com.persist.JsonOps.{Json, jget}
import scalaj.http.{Http, HttpResponse}

class HttpRequestTopic( ipTopics: String, portTopicsKafkaManager: Int, portTopics: Int) {

  def httpGetTopicsString():String= {
    val url=  s"http://${ipTopics}:${portTopics}/topics/"
    try{
      io.Source.fromURL(url).mkString
    }catch{
      case e => throw new Exception(s"Cannot connect to ${url}. Exception message: ${'"'}${e.getMessage}${'"'}")
    }
  }

  def httpDeletetTopic( clusterName:String, topicName: String ): HttpResponse[String] = {
    val response = Http(s"http://${ipTopics}:${portTopicsKafkaManager.toString}/clusters/${clusterName}/topics/delete?t=${topicName}")
      .postForm
      .param("topic",topicName)
      .asString

    if(response.code == 200) {
      println(s"SUCCESS: Delete topic ${'"'}${topicName}${'"'} ")
    }else{
      val parseJson =Json(response.body)
      val message = jget(parseJson, "message")
      println(s"FAILURE: Cannot delete topic ${'"'}${topicName}${'"'} : ${message}")
    }
    response
  }
}
