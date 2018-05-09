package com.snowplowanalytics.bgoctransformer

import scalaz._
import Scalaz._

import com.snowplowanalytics.maxmind.iplookups.IpLookupResult
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.{ClientAttributes, IpLookupsEnrichment, UserAgentUtilsEnrichment}

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Transformer {

  val UriColumns = List("WT.tz", "WT.bh", "WT.ul", "WT.cd",
    "WT.sr", "WT.jo", "WT.ti", "WT.js", "WT.jv", "WT.ct", "WT.bs",
    "WT.fv", "WT.slv", "WT.tv", "WT.dl", "WT.ssl", "WT.es",
    "WT.cg_s", "WT.cg_n", "WT.ce", "WT.vt_f_tlv", "WT.vt_f_tlh",
    "WT.vt_f_d", "WT.vt_f_s", "WT.vtvs", "WT.vtid", "creator",
    "level1", "level2", "level3", "level4", "level5", "level6",
    "current_page", "WT.co_d", "WT.vt_tlh", "WT.vt_tlv", "WT.co",
    "WT.vt_s", "WT.vt_d", "WT.vt_sid", "WT.co_f", "WT.hdr.Host",
    "WT.hdr.Accept", "WT.ac", "WT.ad", "WT.mc_id")

  case class LogLine(date: String,
                     time: String,
                     cIp: String,
                     csUsername: String,
                     csHost: String,
                     csMethod: String,
                     csUriStem: String,
                     csUriQuery: String,
                     csStatus: String,
                     csBytes: String,
                     csVersion: String,
                     csUserAgent: String,
                     csCookies: String,
                     csReferer: String,
                     dcsId: String) {

    def uriColumns: List[(String, String)] = {
      val transformed = queryTransform(csUriQuery).toMap
      UriColumns.map(col => (col, transformed.getOrElse(col, "")))
    }

    /** Transform logline into output columns */
    def getColumns(ipEnrichment: IpLookupsEnrichment): Either[String, List[String]] = {
      val result = for {
        ipEnrichedCols <- ipEnrichment.extractIpInformation(cIp).map(getLocation)
        uauCols <- UserAgentUtilsEnrichment.extractClientAttributes(csUserAgent).map(getUserAgent)
      } yield {
        val columns = List("date" -> date, "time" -> time, "cIp" -> cIp, "csHost" -> csHost,
          "csMethod" -> csMethod, "csUriStem" -> csUriStem, "csUriQuery" -> csUriQuery,
          "csStatus" -> csStatus, "csUserAgent" -> csUserAgent, "csCookies" -> csCookies,
          "csReferer" -> csReferer, "dcsId" -> dcsId)

        val uriCols = UriColumns zip UriColumns.map(uriColumns.toMap) // Sort in same order they're in UriColumns

        (columns ++ uriCols ++ ipEnrichedCols ++ uauCols).map(_._2)
      }

      result.toEither.leftMap(e => s"Enrichment failed: [$e]")
    }
  }

  def transform(ipLookupsEnrichment: IpLookupsEnrichment)(line: String): Either[String, List[String]] =
    for {
      logLine <- Transformer.parse(line)
      cols <- logLine.getColumns(ipLookupsEnrichment)
    } yield cols

  /** Get KV pairs from query parameters */
  def queryTransform(query: String): List[(String, String)] = {
    query.split("&", -1).toList.flatMap(s => s.split("=", -1).toList match {
      case List(k, v) => Some((k, v))
      case _ => None
    })
  }

  def parse(line: String): Either[String, LogLine] =
    line.split(" ", -1).toList match {
      case List(d, t, ip, un, h, m, ust, uq, st, b, v, ua, c, r, id) =>
        Right(LogLine(date = d, time = t, cIp = ip, csUsername = un, csHost = h, csMethod = m,
          csUriStem = ust, csUriQuery = uq, csStatus = st, csBytes = b, csVersion = v,
          csUserAgent = ua, csCookies = c, csReferer = r, dcsId = id))
      case _ =>
        Left(s"Line [$line] doesn't conform format")
    }

  def getLocation(ipLookupsResult: IpLookupResult): List[(String, String)] = {
    val (location, isp, organization, domain, netspeed) = ipLookupsResult

    List(
      "geo_country" -> location.map(_.countryCode).getOrElse(""),
      "geo_region" -> location.flatMap(_.region).getOrElse(""),
      "geo_city" -> location.flatMap(_.city).getOrElse(""),
      "geo_zipcode" -> location.flatMap(_.postalCode).getOrElse(""),
      "geo_latitude" -> location.map(_.latitude.toString).getOrElse(""),
      "geo_longitude" -> location.map(_.longitude.toString).toString,
      "geo_region_name" -> location.flatMap(_.regionName).getOrElse(""),
      "geo_timezone" -> location.flatMap(_.timezone).getOrElse(""),
      "ip_isp" -> isp.getOrElse(""),
      "ip_organization" -> organization.getOrElse(""),
      "ip_domain" -> domain.getOrElse(""),
      "ip_netspeed" -> netspeed.getOrElse("")
    )
  }

  /** Get UA enrichment columns  */
  def getUserAgent(clientAttributes: ClientAttributes): List[(String, String)] = {
    List(
      "br_name"   -> clientAttributes.browserName,
      "br_family" -> clientAttributes.browserFamily,
      "br_version" -> clientAttributes.browserVersion.getOrElse(""),
      "br_type" -> clientAttributes.browserType,
      "br_renderengine" -> clientAttributes.browserRenderEngine,
      "os_name" -> clientAttributes.osName,
      "os_family" -> clientAttributes.osFamily,
      "os_manufacturer" -> clientAttributes.osManufacturer,
      "dvce_type"  -> clientAttributes.deviceType,
      "dvce_ismobile" -> clientAttributes.deviceIsMobile.toString
    )
  }

  // Get columns in exact order they will appear in output
  val columns = List("date", "time", "cIp", "csHost", "csMethod", "csUriStem",
    "csUriQuery", "csStatus", "csUserAgent", "csCookies", "csReferer", "dcsId")
  val ipEnrichedCols = getLocation((None, None, None, None, None)).map(_._1)
  val uauCols = getUserAgent(ClientAttributes("", "", None, "", "", "", "", "", "", false)).map(_._1)

  /** Get column name in order they will appear in output */
  def getColumns: List[String] = 
    columns ++ UriColumns ++ ipEnrichedCols ++ uauCols

  def getSchema = StructType(getColumns.map(columnName => StructField(columnName, StringType)))
}
