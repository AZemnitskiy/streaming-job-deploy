import java.io.File

import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

class SchemaRegistryTest extends FunSuite {
  val pathCreating = System.getProperty("user.dir")+File.separator+ "src"+File.separator+"test"+File.separator+"resources"+File.separator+"streaming-job-workflow"
  val path= pathCreating.replaceAll("\\\\","/")

  test("SchemaRegistry.MissingSchemaTopicsYMLPresent") {
    println("MissingSchemaTopicsYMLPresent")
    //Added topics shampoo.yml,but no schema file for it
    val conf = ConfigFactory.load()
    val ip = conf.getString("schemas.host-ip")
    val port = conf.getInt("schemas.host-port")
    val dirSchema = s"${path}/missing-schema-for-topics/schemas"//conf.getString("schemas.folder")
    val dirTopics = s"${path}/missing-schema-for-topics/topics"
    val caught =
      intercept[Exception] { // Result type: IndexOutOfBoundsException
        Deploy.registerSchema( ip, port, dirSchema, dirTopics)
      }
    println("")
    assert(caught.getMessage== "Missing Schema for topics")
  }

  test("SchemaRegistry.ExtraSchemaNoTopicsYMLAssociated") {
    println("ExtraSchemaNoTopicsYMLAssociated")
    //ExtraSchemaNoTopicsYMLAssociated, in this case schema is not register on Kafka
    //Product schema is present, but no product.yml in topics
    val conf = ConfigFactory.load()
    val ip = conf.getString("schemas.host-ip")
    val port = conf.getInt("schemas.host-port")
    val dirSchema = s"${path}/extra-schema-no-topics/schemas"//conf.getString("schemas.folder")
    val dirTopics = s"${path}/extra-schema-no-topics/topics"

    Deploy.registerSchema( ip, port, dirSchema, dirTopics)
    val subjectsString = io.Source.fromURL(s"http://${ip}:${port}/subjects").mkString

    assert(!subjectsString.contains("product"))
    assert(subjectsString==="[\"customer\"]") //test subject correctly push on Kafka
  }

}