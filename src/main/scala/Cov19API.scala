import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.{Http, server}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes.{BadRequest, InternalServerError}
import akka.http.scaladsl.model.{DateTime, HttpMethods, HttpRequest, HttpResponse}
import akka.http.scaladsl.server.{Directives, ExceptionHandler, Route}
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.sun.jdi.InvocationException
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import java.lang.reflect.InvocationTargetException
import java.time.LocalDate
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.util.Locale
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

class ExternalAPIException(msg: String) extends Exception(msg)

final case class Cov19Info(ID: String, Country: String, CountryCode: String, Province: String, City: String, CityCode: String, Lat: String, Lon: String, Confirmed: Int, Deaths: Int, Recovered: Int, Active: Int, Date: String)
final case class Cov19DayCountryInfo(Country: String, NewConfirmed: Int, NewDeaths: Int, NewRecovered: Int, Active: Int, Date: String)
final case class Country(Country: String, Slug: String, ISO2: String)
final case class ErrorResponse(message: String, success: Boolean)

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val cov19InfoFormat: RootJsonFormat[Cov19Info] = jsonFormat13(Cov19Info.apply)
  implicit val cov19DayCountryInfoFormat: RootJsonFormat[Cov19DayCountryInfo] = jsonFormat6(Cov19DayCountryInfo.apply)
  implicit val countryFormat: RootJsonFormat[Country] = jsonFormat3(Country.apply)
}

object Server extends Directives with JsonSupport {
  implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "SingleRequest")
  implicit val executionContext: ExecutionContextExecutor = system.executionContext
  val inFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val externalApiFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")

  val confirmedAsc: (Cov19DayCountryInfo, Cov19DayCountryInfo) => Boolean = (el1: Cov19DayCountryInfo, el2: Cov19DayCountryInfo) => LocalDate.parse(el1.Date, inFormatter).isBefore(LocalDate.parse(el2.Date, inFormatter)) | (el1.NewConfirmed < el2.NewConfirmed & LocalDate.parse(el1.Date, inFormatter).isEqual(LocalDate.parse(el2.Date, inFormatter)))
  val confirmedDesc: (Cov19DayCountryInfo, Cov19DayCountryInfo) => Boolean = (el1: Cov19DayCountryInfo, el2: Cov19DayCountryInfo) => el1.NewConfirmed > el2.NewConfirmed | LocalDate.parse(el1.Date, inFormatter).isBefore(LocalDate.parse(el2.Date, inFormatter))
  val deathsAsc: (Cov19DayCountryInfo, Cov19DayCountryInfo) => Boolean = (el1: Cov19DayCountryInfo, el2: Cov19DayCountryInfo) => LocalDate.parse(el1.Date, inFormatter).isBefore(LocalDate.parse(el2.Date, inFormatter)) | (el1.NewDeaths < el2.NewDeaths & LocalDate.parse(el1.Date, inFormatter).isEqual(LocalDate.parse(el2.Date, inFormatter)))
  val deathsDesc: (Cov19DayCountryInfo, Cov19DayCountryInfo) => Boolean = (el1: Cov19DayCountryInfo, el2: Cov19DayCountryInfo) => el1.NewDeaths > el2.NewDeaths | LocalDate.parse(el1.Date, inFormatter).isBefore(LocalDate.parse(el2.Date, inFormatter))
  val recoveredAsc: (Cov19DayCountryInfo, Cov19DayCountryInfo) => Boolean = (el1: Cov19DayCountryInfo, el2: Cov19DayCountryInfo) => el1.NewRecovered < el2.NewRecovered | LocalDate.parse(el1.Date, inFormatter).isBefore(LocalDate.parse(el2.Date, inFormatter))
  val recoveredDesc: (Cov19DayCountryInfo, Cov19DayCountryInfo) => Boolean = (el1: Cov19DayCountryInfo, el2: Cov19DayCountryInfo) => el1.NewRecovered > el2.NewRecovered | LocalDate.parse(el1.Date, inFormatter).isBefore(LocalDate.parse(el2.Date, inFormatter))
  val activeAsc: (Cov19DayCountryInfo, Cov19DayCountryInfo) => Boolean = (el1: Cov19DayCountryInfo, el2: Cov19DayCountryInfo) => el1.Active < el2.Active | LocalDate.parse(el1.Date, inFormatter).isBefore(LocalDate.parse(el2.Date, inFormatter))
  val activeDesc: (Cov19DayCountryInfo, Cov19DayCountryInfo) => Boolean = (el1: Cov19DayCountryInfo, el2: Cov19DayCountryInfo) => el1.Active > el2.Active | LocalDate.parse(el1.Date, inFormatter).isBefore(LocalDate.parse(el2.Date, inFormatter))

  val countriesAvailable: mutable.HashSet[String] = mutable.HashSet()

  implicit def exceptionHandler: ExceptionHandler =
    server.ExceptionHandler {
      case _: InterruptedException =>
        extractUri { uri =>
          println(s"Request to $uri could not be handled normally")
          complete(HttpResponse(InternalServerError, entity = "Server processing error"))
        }
      case ex: IllegalArgumentException =>
        extractUri { uri =>
          println(s"Request to $uri could not be handled normally")
          complete(HttpResponse(BadRequest, entity = s"Error ${ex.getMessage}"))
        }
      case _: concurrent.TimeoutException =>
        extractUri { uri =>
          println(s"Request to $uri could not be handled normally")
          complete(HttpResponse(InternalServerError, entity = "Server processing error"))
        }
      case _: DateTimeParseException =>
        extractUri { uri =>
          println(s"Request to $uri could not be handled normally")
          complete(HttpResponse(BadRequest, entity = "Incorrect date format"))
        }
      case ex: ExternalAPIException =>
        extractUri { uri =>
          println(s"Request to $uri could not be handled normally cause of external API")
          complete(HttpResponse(InternalServerError, entity = ex.getMessage))
        }
    }

  val routeApi: Route =
    handleExceptions(exceptionHandler) {
      path("api") {
        get {
          parameters("from", "to", "countries","sortParam".withDefault("Confirmed"), "sortDir".withDefault("ASC")) { (from, to, countries, sortParam, sortDir) =>
            complete(getCases(from, to, countries, sortParam, sortDir))
          }
        }
      }
    }

  val bindingFuture: Future[Http.ServerBinding] = Http().newServerAt("localhost", 8080).bind(routeApi)

  def getCases(from: String, to: String, countriesStr: String, sortParam: String, sortDir: String): Array[Cov19DayCountryInfo] = {

    val countries = countriesStr.split(",").map(country => country.toLowerCase(Locale.US))
    countries.foreach(country =>
      if (!countriesAvailable.contains(country)) throw IllegalArgumentException(s"Country of $country is not supported")
    )
    val dateFrom = LocalDate.parse(from, inFormatter).minusDays(1).toString
    val dateTo = LocalDate.parse(to, inFormatter).toString


    val desc: Boolean = sortDir.toLowerCase(Locale.US).equals("desc")
    val sortAlg: (Cov19DayCountryInfo, Cov19DayCountryInfo) => Boolean = sortParam.toLowerCase(Locale.US) match {
      case "confirmed" => if (desc) confirmedDesc else confirmedAsc
      case "deaths" => if (desc) deathsDesc else deathsAsc
      case "active" => if (desc) activeDesc else activeAsc
      case "recovered" => if (desc) recoveredAsc else recoveredDesc
      case _ => throw IllegalArgumentException("No such parameter")
    }

    val externalResponses = new ArrayBuffer[Array[Cov19Info]]()

    var failed = false

    for (country <- countries) {
      val response = Http().singleRequest(HttpRequest(method = HttpMethods.GET, uri = s"https://api.covid19api.com/country/$country?from=$dateFrom&to=$dateTo"))
      val responseAsInfo: Future[Array[Cov19Info]] = Unmarshal(Await.result(response, Duration.Inf).entity).to[Array[Cov19Info]]
      responseAsInfo.onComplete {
        case Success(resp) => externalResponses.addOne(resp)
        case Failure(_) =>
          println("failed to load")
          failed = true
      }
      Await.ready(responseAsInfo, Duration.Inf)
    }

    if (failed) throw ExternalAPIException("Too many requests to external API")

    val response = ArrayBuffer[Cov19DayCountryInfo]()

    for (countryCases <- externalResponses) {
      for (index <- 1 until countryCases.length){
        response.addOne(
          Cov19DayCountryInfo(
            countryCases(index).Country,
            countryCases(index).Confirmed - countryCases(index-1).Confirmed,
            countryCases(index).Deaths - countryCases(index-1).Deaths,
            countryCases(index).Recovered - countryCases(index-1).Recovered,
            countryCases(index).Active,
            LocalDate.parse(countryCases(index).Date, externalApiFormatter).format(inFormatter)
          )
        )
      }
    }

    response.sortWith(sortAlg).toArray

  }

  def main(args: Array[String]): Unit = {
    val responseFuture = Http().singleRequest(HttpRequest(method = HttpMethods.GET, uri = "https://api.covid19api.com/countries"))
    val responseDataFuture: Future[Array[Country]] = Unmarshal(Await.result(responseFuture, Duration.Inf).entity).to[Array[Country]]
    countriesAvailable.addAll(
      Await.result(responseDataFuture, Duration.Inf).map(el => el.Slug)
    )
    println("Server started")
  }
}
