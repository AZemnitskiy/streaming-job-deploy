class HttpRequestSchema(ip: String, port: Int) {

  def httpGetSubjectList():String= {
    val url= s"http://${ip}:${port}/subjects"
    try{
      io.Source.fromURL(url).mkString
    }catch{
      case e => throw new Exception(s"Cannot connect to ${url} . Exception message: ${'"'}${e.getMessage}${'"'}")
    }
  }

}
