package com.swiggy

import com.swiggy.SimpleApplication.{spark, sparkConfigUtil}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{JsDefined, JsUndefined, Json}
import io.delta.tables.DeltaTable

import scala.util.{Failure, Success, Try}

//Not tested code
object InstamartTransformation {

  def process(sourceDf: DataFrame, id: Long)(implicit
      spark: SparkSession
  ): Unit = {

    def getGmvTotal(event: String) = {
      Try {
        (Json.parse(event) \\ "bill")
          .map(x => {
            val itemBasePrice = Try {
              x \ "itemBasePrice"
            } match {
              case Failure(ex) =>
                0.0
              case Success(value) =>
                value match {
                  case _: JsDefined   => value.as[Float]
                  case _: JsUndefined => 0.0
                }
            }
            val quantity = Try {
              x \ "quantity"
            } match {
              case Failure(ex) =>
                0.0
              case Success(value) =>
                value match {
                  case _: JsDefined   => value.as[Float]
                  case _: JsUndefined => 0.0
                }
            }
            itemBasePrice * quantity
          })
          .sum
      } match {
        case Success(v)  => v
        case Failure(ex) => 0.0
      }
    }

    val gmvUdf = udf(getGmvTotal _)
    spark.udf.register("gmvUdf", gmvUdf)

    sourceDf.createOrReplaceTempView("sourceDf")
    val dedupedDf = sourceDf.sparkSession
      .sql(
        "select * from (select *, ROW_NUMBER() OVER (Partition By id Order By updated_at desc) as rn from sourceDf) t1 where rn = 1"
      )
      .drop("rn")
    dedupedDf.createOrReplaceTempView("dedupedDf")

    val dfWithIstCol = dedupedDf.sparkSession.sql("""select *,
                                                    from_utc_timestamp(updated_at, 'Asia/Kolkata') as updated_at_ist,
                                                    from_utc_timestamp(created_at, 'Asia/Kolkata') as created_at_ist,
                                                    unix_timestamp(from_utc_timestamp(updated_at, 'Asia/Kolkata'))*1000 as updated_at_ist_epoch,
                                                    get_json_object(metadata, '$.type') as dp_order_type,
                                                    get_json_object(metadata, '$.storeId') as store_id,
                                                    get_json_object(metadata, '$.deliveryType') as delivery_type,
                                                    case when get_json_object(metadata, '$.deliveryType') ="SLOTTED" then
                                                    concat(extract(hour from from_utc_timestamp (from_unixtime(get_json_object(metadata, '$.deliveryDetails.startTime')), 'Asia/Kolkata') ),'-',
                                                    extract(hour from from_utc_timestamp (from_unixtime(get_json_object(metadata, '$.deliveryDetails.endTime')), 'Asia/Kolkata')))  else null
                                                    end as delivery_slot,
                                                    get_json_object(metadata, '$.storeInfo.cityId') as city,
                                                    get_json_object(metadata, '$.storeInfo.areaId') as zone,
                                                    gmvUdf(get_json_object(metadata, '$.bill.billedItems')) as gmv,
                                                    unix_timestamp() as  processing_time
                                                    from dedupedDf """)

    dfWithIstCol.createOrReplaceTempView("dfWithIstCol")

    val primaryKey = "id"
    val targetPath = sparkConfigUtil.getTargetPath.get.get

    val dropDuplicatesDf = dfWithIstCol.dropDuplicates(s"$primaryKey")

    dropDuplicatesDf.show()

    val deltaTable = DeltaTable.forPath(targetPath)
    deltaTable
      .as("t")
      .merge(dropDuplicatesDf.as("s"), s"s.$primaryKey = t.$primaryKey")
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute()
  }
}
