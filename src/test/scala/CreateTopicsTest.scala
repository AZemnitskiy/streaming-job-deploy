import java.io.File
import Deploy.{getListOfFiles, registerSchema}
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

class CreateTopicsTest extends FunSuite {
  //TODO path
  //val test = ResourcesUtil.getResourcePath()
  //val t =getClass.getClassLoader.getResource()
  val pathCreating = System.getProperty("user.dir")+File.separator+ "src"+File.separator+"test"+File.separator+"resources"+File.separator+"streaming-job-workflow"
  val path= pathCreating.replaceAll("\\\\","/")

  val conf = ConfigFactory.load()
  val ip = conf.getString("schemas.host-ip")
  val port = conf.getInt("schemas.host-port")
  val ipTopics = conf.getString("topics.host-ip")
  val portTopicsKafkaManager = conf.getInt("topics.host-port-kafka-manager")
  val portTopics = conf.getInt("topics.host-port")
  val clusterName = conf.getString("topics.cluster")
  val zkHosts = conf.getString("topics.zkHosts")
  val kafkaVersion = conf.getString("topics.kafkaVersion")

  val requestSchema = new HttpRequestSchema(ip, port)

  test("Topic.CreateTopicWitNoSchema") {
    //Create a topic with no schema, code should through an exception
    println("\n")
    println("Create Topic With No Schema")
    val dirSchema = s"${path}/missing-schema-for-topics/schemas"
    val dirTopics = s"${path}/missing-schema-for-topics/topics"

    val listFilesTopicsFromRepo = getListOfFiles(dirTopics)

    //val topicString1 = io.Source.fromURL(s"http://${ip}:8084/topics/").mkString
    val schemaRegistered = registerSchema(requestSchema, ip, port, dirSchema, listFilesTopicsFromRepo)

    val caught =
      intercept[Exception] { // Result type: IndexOutOfBoundsException
        Deploy.createOrUpdateTopics(requestSchema,ip, port, ipTopics, portTopicsKafkaManager, zkHosts, kafkaVersion, portTopics, dirSchema, dirTopics, clusterName, listFilesTopicsFromRepo, schemaRegistered)
      }
    println("")
    assert(caught.getMessage.contains("Cannot create topic"))

    val topicString = io.Source.fromURL(s"http://${ipTopics}:8084/topics/").mkString
    assert(!topicString.contains("user.shampoo")) //TODO WHY THIS TEST DOES NOT SEND AN ERROR OR WARNONG AS NO SCHEMA REGISTRY???
  }

    test("Topic.CreateTopicWrongSchema") {
    //Create a topic "client" with schema not register
    println("\n")
    println("Create Topic Wrong Schema")
    val dirSchema = s"${path}/schema-wrong-associated-topic/schemas"//conf.getString("schemas.folder")
    val dirTopics = s"${path}/schema-wrong-associated-topic/topics"

    val listFilesTopicsFromRepo = getListOfFiles(dirTopics)

    val topicString1 = io.Source.fromURL(s"http://${ip}:8084/topics/").mkString
    val schemaRegistered =registerSchema( requestSchema,ip, port, dirSchema, listFilesTopicsFromRepo)

    val caught =
      intercept[Exception] { // Result type: IndexOutOfBoundsException
        Deploy.createOrUpdateTopics( requestSchema,ip, port, ipTopics, portTopicsKafkaManager,zkHosts, kafkaVersion,  portTopics, dirSchema, dirTopics, clusterName, listFilesTopicsFromRepo, schemaRegistered)
      }
    println("")
    assert(caught.getMessage.contains( "Cannot create topic" ))

    val topicString = io.Source.fromURL(s"http://${ipTopics}:8084/topics/").mkString
    assert(!topicString.contains("user.client"))
  }

  test("Topic.CreateTopicWithDifferentSchemaName") {
    //Create a topic "user.card" with schema product
    println("CreateTopic With Different Schema Name")
    val dirSchema = s"${path}/one-topic-several-schemas/schemas"//conf.getString("schemas.folder")
    val dirTopics = s"${path}/one-topic-several-schemas/topics"
    val listFilesTopicsFromRepo = getListOfFiles(dirTopics)
    val topicString1 = io.Source.fromURL(s"http://${ip}:8084/topics/").mkString
    val schemaRegistered =registerSchema( requestSchema,ip, port, dirSchema, listFilesTopicsFromRepo)

    Deploy.createOrUpdateTopics(requestSchema, ip, port, ipTopics, portTopicsKafkaManager,zkHosts, kafkaVersion,  portTopics, dirSchema, dirTopics, clusterName, listFilesTopicsFromRepo, schemaRegistered)

    //Thread.sleep(1000)
    val topicString = io.Source.fromURL(s"http://${ipTopics}:8084/topics/").mkString
    val bool = topicString.contains("card")
    assert(bool)
  }
}