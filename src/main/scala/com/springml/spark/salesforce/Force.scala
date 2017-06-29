/*
 * Copyright 2015 - 2017, oolong  
 * Author  : Kagan Turgut, Oolong Inc.
 * Contributors:
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.springml.spark.salesforce

import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.mutable.StringBuilder

import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.NameValuePair
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.httpclient.methods.PostMethod
import org.json.JSONObject
import org.json.JSONArray

import org.apache.spark.sql.SQLContext

import org.apache.log4j.Logger
 

/* This uses the FORCE.com REST API
*/

class Force (val accessToken:String, val apiVersion:String, val baseUrl:String) {
  
  @transient private val logger = Logger.getLogger(classOf[Force])
  
  def httpGet(
    accessToken: String,
    endpoint: String,
    query: String = "NA",
    prettyPrint: Boolean = false) = {
      val httpclient = new HttpClient()
      val gm = new GetMethod(endpoint)
  
      //set the token in the header
      gm.setRequestHeader("Authorization", "Bearer " + accessToken)
      gm.setRequestHeader("Content-Type", "application/json")
  
      if (prettyPrint) {
        gm.setRequestHeader("X-PrettyPrint", "1")
      }
  
      //logger.trace(s"HttpRequest,type=GET,endpoint=$endpoint")
  
      //set the optional SOQL as a query param
      if (query != "NA") {
        val params = new Array[NameValuePair](1)
        params(0) = new NameValuePair("q", query)
        gm.setQueryString(params)
      }
  
      val responseCode = httpclient.executeMethod(gm)
  
      if (responseCode == 200) {
        gm.getResponseBodyAsString
      } else {
        //400 may produce the following response body:
        //{"message":"entity type ActivityHistory does not support query","errorCode":"INVALID_TYPE_FOR_OPERATION"}
        val statusText = gm.getStatusText
        val responseBody = gm.getResponseBodyAsString
        logger.error(s"HttpRequest,type=GET,endpoint=$endpoint,responseCode=$responseCode,statusText=$statusText,responseBody=$responseBody")
        responseBody
      }
  }

  def describeObject(objName: String): JSONObject = {
    val describeObjUrl = s"$baseUrl/services/data/v${apiVersion}/sobjects/${objName}/describe"
    val describeObj = httpGet(accessToken, describeObjUrl)
    val describeObjJson = new JSONObject(describeObj)

    describeObjJson
  }

 
  def getObjectList() = {

    val queryEndpoint = s"$baseUrl/services/data/v$apiVersion/sobjects"

    httpGet(accessToken, queryEndpoint)
  }
  

  def getObjectNames(ignore:Seq[String] = Seq()):List[String] = {
    
    val objList = getObjectList();
    val objListJson = new JSONObject(objList)
    val s:JSONArray = objListJson.get("sobjects").asInstanceOf[JSONArray]
    
    var names = List[String]()
    
    for (i <- 0 to s.length() - 1) {
      val obj = s.getJSONObject(i)
      val objName = obj.getString("name")
      if (!ignore.contains(objName)) 
        names = objName :: names
    }
    logger.trace(s"Filtered object names: $names");
    names
  }
  /**
   * (used, remaining, percent used)
   */
  def getSalesforceQuotaUsage():(Integer, Integer,Integer) = {
    
    val queryEndpoint = s"$baseUrl/services/data/v$apiVersion/limits"
    
    val result = httpGet(accessToken, queryEndpoint)
    val limits = new JSONObject(result)
    val dailyAPIReqs = limits.getJSONObject("DailyApiRequests");
    val max:Float = dailyAPIReqs.getInt("Max")
    val remaining = dailyAPIReqs.getInt("Remaining")
    val used:Float = max - remaining
    val percent:Float = (used/max)*100
    val percentInt = percent.toInt
    val usedInt = used.toInt
    logger.info(s"Quota Usage Summary: Daily API limit: $max, remaining: $remaining percent used: $percentInt")
    (usedInt,remaining,percentInt)
  }


}

object Force {
  
 @transient private val logger = Logger.getLogger(classOf[Force])
    
  private def getAccessToken(
    sfdcClientId: String,
    sfdcClientSecret: String,
    sfdcUsername: String,
    sfdcPassword: String,
    sfdcSecurityToken: String,
    sfdcLoginTokenUrl: String = "https://login.salesforce.com/services/oauth2/token") = {

    val httpclient = new HttpClient()
    val post = new PostMethod(sfdcLoginTokenUrl)
    post.addParameter("grant_type", "password")
    post.addParameter("client_id", sfdcClientId)
    post.addParameter("client_secret", sfdcClientSecret)
    post.addParameter("username", sfdcUsername)
    post.addParameter("password", s"${sfdcPassword}${sfdcSecurityToken}")

    val responseCode = httpclient.executeMethod(post)

    if (responseCode == 200) {
      val responseBody = post.getResponseBody
      val responseBodyStr = post.getResponseBodyAsString
      post.getResponseBodyAsString

      var accessToken: String = "";

      try {
        val json = new JSONObject(responseBodyStr)
        accessToken = json.getString("access_token")
        val issuedAt = json.getString("issued_at")
        accessToken
      } catch {
        case je:
          Exception =>
          logger.error("ERROR Could not deserialize json: ",je)
          throw je
      }
    } else {
      println(s"ERROR HTTP Request Failed.  Code: ${responseCode}")
      throw new Exception(s"ERROR HTTP POST Request Failed.  Code: ${responseCode}")
    }
  }
  
  
  def apply(userName:String, password:String, clientId:String, clientSecret:String, securityToken:String):Force = {
    val accessToken = getAccessToken ( clientId,clientSecret,userName,password,securityToken)
    new Force(accessToken, "38.0", "https://ooequipment.my.salesforce.com")
  }
    
  def apply(sqlContext:SQLContext):Force = {
    val conf = sqlContext.sparkSession.conf;
    val userName =  conf.get("spark.oolong.salesforce.username")
    val password =  conf.get("spark.oolong.salesforce.password")
    val clientId =  conf.get("spark.oolong.salesforce.clientId")
    val clientSecret =  conf.get("spark.oolong.salesforce.clientSecret")
    val securityToken =  conf.get("spark.oolong.salesforce.securityToken")
    apply(userName,password, clientId,clientSecret,securityToken)
  }  
}