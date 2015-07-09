package com.springml.spark.salesforce

import com.sforce.soap.partner.{Connector, PartnerConnection}
import com.sforce.ws.ConnectorConfig
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider}

import scala.util.Try

/**
 * Entry point to the save part
 */
class DefaultSource extends CreatableRelationProvider{





  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {

    val username = parameters.getOrElse("username", sys.error("'username' must be specified for sales force."))
    val password = parameters.getOrElse("password", sys.error("'password' must be specified for sales force."))
    //val password = parameters.getOrElse("endpoint", sys.error("'password' must be specified for sales force."))


    val metaDataJson = Utils.generateMetaString(data.schema,"test")
    println(metaDataJson)



    /*val config = new ConnectorConfig()
    config.setUsername(username)
    config.setPassword(password)

    val connection = Connector.newConnection(config)
    println(config.getAuthEndpoint)*/








    return  null
  }
}
