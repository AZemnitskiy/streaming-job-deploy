class HttpRequestTopic( ipTopics: String, portTopicsKafkaManager: Int, portTopics: Int) {

  def httpGetTopicsString():String= {
    val url=  s"http://${ipTopics}:${portTopics}/topics/"
    try{
      io.Source.fromURL(url).mkString
    }catch{
      case e => throw new Exception(s"Cannot connect to ${url}. Exception message: ${'"'}${e.getMessage}${'"'}")
    }
  }

}
