import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

class MainTest extends FunSuite {
  val argArray = new Array[String](1)
  argArray(0)= "example.conf"
  Deploy.main(argArray)
}
