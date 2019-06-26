import java.io.File
import com.typesafe.config.ConfigFactory
import FilesUtils._


object Deploy {

  val usage = """
    Usage: deploy application.conf
  """

  def main(args: Array[String] ): Unit = {
    try {
      val myConfigFile = new File(args(0))
      val conf = ConfigFactory.parseFile(myConfigFile)

      val path = conf.getString("target.folder")
      val ip = conf.getString("schemas.host-ip")
      val port = conf.getInt("schemas.host-port")

      val pathFolder = new File(path)
      val dirSchema1 = new File(conf.getString("schemas.folder"))
      val dirSchema = (pathFolder.getPath + File.separator + dirSchema1.getPath).replaceAll("\\\\", "/")

      val ipTopics = conf.getString("topics.host-ip")
      val portTopicsKafkaManager = conf.getInt("topics.host-port-kafka-manager")
      val portTopics = conf.getInt("topics.host-port")
      val dirTopic1 = new File(conf.getString("topics.folder"))
      val dirTopics = (pathFolder.getPath + File.separator + dirTopic1.getPath).replaceAll("\\\\", "/")
      val clusterName = conf.getString("topics.cluster")
      val zkHosts = conf.getString("topics.zkHosts")
      val kafkaVersion = conf.getString("topics.kafkaVersion")
      val prefix = conf.getString("topics.prefix")

      val requestSchema= new HttpRequestSchema(ip,port)
      val requestTopic= new HttpRequestTopic(ipTopics,portTopicsKafkaManager, portTopics)

      val schemas = new Schemas ( ip, port, dirSchema)
      val topics = new Topics ( ipTopics, portTopicsKafkaManager, portTopics, dirTopics, clusterName, zkHosts, kafkaVersion, prefix)

      val listFilesTopicsFromRepo = getListOfFiles(dirTopics)

      //##############Register Schema #################
      val schemaRegistered = schemas.registerSchema(requestSchema, listFilesTopicsFromRepo)

      //##############Publish Topics #################
      //Once Schema is registered, push topics on Kafka:
      //-if topics already exist update it
      //-if new topics, publish it
      topics.createOrUpdateTopics(requestSchema, requestTopic, listFilesTopicsFromRepo, schemaRegistered)
    }catch{
      case e: Exception => {println("exception caught: " + e.getMessage)
                            System.exit(-1)
                            }

    }
  }








/*
  case class VersionContent(json: String)
  case class SubjectValue( version: Map[Int, VersionContent])
  case class SubjectKey( name: String)
*/





}



