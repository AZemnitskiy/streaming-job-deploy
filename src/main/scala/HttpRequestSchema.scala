import com.persist.JsonOps.{Json, jget}
import scalaj.http.{Http, HttpResponse}

class HttpRequestSchema(ip: String, port: Int) {

  def httpGetSubjectList():String= {
    val url= s"http://${ip}:${port}/subjects"
    try{
      io.Source.fromURL(url).mkString
    }catch{
      case e => throw new Exception(s"Cannot connect to ${url} . Exception message: ${'"'}${e.getMessage}${'"'}")
    }
  }


  def httpGetAllInfoAboutSubject(x: String):String= {
    val url= s"http://${ip}:${port}/subjects/${x}/versions"
    try{
       io.Source.fromURL(url).mkString
    }catch{
      case e => throw new Exception(s"Cannot connect to ${url} . Exception message: ${'"'}${e.getMessage}${'"'}")
    }
  }


  def httpPostSchemaRegistryForEachVersion( subject:String, versionData:Map[Int,String] ): Unit = {
    versionData.foreach( versionDataMap => {
      val t =versionDataMap._2.replace("\"", "\\\"")
      val postData ="{\"schema\": \""+ t +"\"}\n"
      httpPostSchemaRegistry(subject, postData ,versionData.head._1)
    } )
  }

  def httpPostSchemaRegistry( subject:String, postData:String, version: Int ): HttpResponse[String] = {
    val response = Http(s"http://${ip}:${port}/subjects/${subject}/versions")///post
      .header("Content-Type", "application/vnd.schemaregistry.v1+json")
      .postData(postData)
      .asString

    if(response.code == 200) {
      println(s"SUCCESS: Posting schema registry for subject ${'"'}${subject}${'"'} for version ${'"'}${version}${'"'} from repository")
    }else{
      val parseJson =Json(response.body)
      val message =jget(parseJson, "message")
      println(s"FAILURE: Cannot post subject ${'"'}${subject}${'"'} for version ${'"'}${version}${'"'} from repository: ${message} ")
    }
    response
  }

  def httpDeletetSchemaRegistryAndAllVersion( subject:String): HttpResponse[String] = {
    val response = Http(s"http://${ip}:${port}/subjects/${subject}/")
      .method("DELETE")
      .asString

    if (response.code == 200) {
      println(s"Delete schema registry and all version of: ${'"'}${subject}${'"'}")
      response
    }else
    {
      val parseJson =Json(response.body)
      val message =jget(parseJson, "message")
      println(s"FAILURE: Cannot delete subject ${'"'}${subject}${'"'}: ${message} ")
      response
    }
  }





}
