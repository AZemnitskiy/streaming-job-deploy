object ResourcesUtil {

  def getResourcePath() = {
   // val source = getClass.getClassLoader.getResource()
    val source = scala.io.Source.fromURL(getClass.getResource("/"))
    try source.mkString finally source.close()
  }
}
