import java.io.File
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite
import FilesUtils._

class CreateTopicSimpleTest extends FunSuite {
  val pathCreating = System.getProperty("user.dir") + File.separator + "src" + File.separator + "test" + File.separator + "resources" + File.separator + "streaming-job-workflow"
  val path = pathCreating.replaceAll("\\\\", "/")

  val conf = ConfigFactory.load()
  val ip = conf.getString("schemas.host-ip")
  val port = conf.getInt("schemas.host-port")
  val ipTopics = conf.getString("topics.host-ip")
  val portTopicsKafkaManager = conf.getInt("topics.host-port-kafka-manager")
  val portTopics = conf.getInt("topics.host-port")
  val clusterName = conf.getString("topics.cluster")
  val zkHosts = conf.getString("topics.zkHosts")
  val kafkaVersion = conf.getString("topics.kafkaVersion")
  val requestTopic = new HttpRequestTopic(ipTopics,portTopicsKafkaManager,portTopics)
  val requestSchema = new HttpRequestSchema(ip, port)

  test("Topic.CreateTopic") {
    println("Create Topic")
    //Create a topic "customer" that is not already on Kafka
    //Be sure to delete any topic name "customer" on Kafka on cluster "test"
    val dirSchema = s"${path}/extra-schema-no-topics/schemas" //conf.getString("schemas.folder")
    val dirTopics = s"${path}/extra-schema-no-topics/topics"
    val schemas = new Schemas ( ip, port, dirSchema)
    val topics = new Topics ( ipTopics, portTopicsKafkaManager, portTopics, dirTopics, clusterName, zkHosts, kafkaVersion)

    val listFilesTopicsFromRepo = getListOfFiles(dirTopics)

    //val topicString1 = requestTopic.httpGetTopicsString()
    val schemaRegistered = schemas.registerSchema(requestSchema, listFilesTopicsFromRepo)

    topics.createOrUpdateTopics(requestSchema, requestTopic, listFilesTopicsFromRepo, schemaRegistered)
    Thread.sleep(1000)
    val topicString = requestTopic.httpGetTopicsString().toString
    val bool = topicString.contains("customer")
    assert(bool)
  }

  test("Topic.CreateTopicAlreadyExisting") {
    println("Create Topic already existing")
    //Create a topic "customer" that is not already on Kafka
    //Be sure to delete any topic name "customer" on Kafka on cluster "test"
    val dirSchema = s"${path}/extra-schema-no-topics/schemas" //conf.getString("schemas.folder")
    val dirTopics = s"${path}/extra-schema-no-topics/topics"
    val schemas = new Schemas ( ip, port, dirSchema)
    val topics = new Topics ( ipTopics, portTopicsKafkaManager, portTopics, dirTopics, clusterName, zkHosts, kafkaVersion)

    val listFilesTopicsFromRepo = getListOfFiles(dirTopics)

    //val topicString1 = requestTopic.httpGetTopicsString()
    val schemaRegistered = schemas.registerSchema(requestSchema, listFilesTopicsFromRepo)

    topics.createOrUpdateTopics(requestSchema, requestTopic, listFilesTopicsFromRepo, schemaRegistered)
    Thread.sleep(1000)
    val topicString = requestTopic.httpGetTopicsString()
    val bool = topicString.contains("customer")
    assert(bool)
  }
}