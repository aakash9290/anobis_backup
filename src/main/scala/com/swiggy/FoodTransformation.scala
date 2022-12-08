package com.swiggy

import com.swiggy.SimpleApplication.sparkConfigUtil
import io.delta.tables.DeltaTable
import org.apache.spark.sql._

object FoodTransformation {

  def process(sourceDf: DataFrame, id: Long)(
    implicit spark: SparkSession):Unit = {

    // We may get multiple entries for any ID so below code would give us latest entry
    sourceDf.createOrReplaceTempView("sourceDf")

    val dedupedDf = sourceDf.sparkSession.sql("select * from (select *, ROW_NUMBER() OVER (Partition By id Order By updated_on desc) as rn  from sourceDf) t1 where rn = 1").drop("rn")

    dedupedDf.createOrReplaceTempView("dedupedDf")

    val transformedDf = dedupedDf.sparkSession.sql("""select * ,
                                                  from_utc_timestamp(updated_on, 'Asia/Kolkata') as updated_on_ist,
                                                  from_utc_timestamp(created_on, 'Asia/Kolkata') as created_on_ist,
                                                  unix_timestamp(from_utc_timestamp(updated_on, 'Asia/Kolkata'))*1000 as updated_on_ist_epoch,
                                                  case when extract(hour from from_utc_timestamp(created_on, 'Asia/Kolkata'))  in (6,7,8,9,10) then 'Breakfast'
                                                  when extract(hour from from_utc_timestamp(created_on, 'Asia/Kolkata'))  in (11,12,13,14,15) then 'Lunch'
                                                  when extract(hour from from_utc_timestamp(created_on, 'Asia/Kolkata'))  in (16,17,18) then 'Snack'
                                                  when extract(hour from from_utc_timestamp(created_on, 'Asia/Kolkata'))  in (19,20,21,22) then 'Dinner'
                                                  else 'Late Night Dinner'
                                                  end as time_slot,
                                                  get_json_object(order_details, '$.areaDetails.name') as area,
                                                  get_json_object(order_details, '$.cityDetails.name') as city,
                                                  cast(get_json_object(order_details, '$.financeDetails.orderRestaurantBill') as float) as gmv_total,
                                                  get_json_object(order_details, '$.additionalInfo.cancellationMeta.cancelOrderRequest.responsible_id') as canceled_reason,
                                                  get_json_object(order_details, '$.orderType') as order_type,
                                                  unix_timestamp()* 1000 as processing_time
                                                  from dedupedDf""")

    transformedDf.createOrReplaceTempView("transformedDf")

    transformedDf.show()

    // @TODO Add code to handle user wants to use output table sync or not [Optional] Param 6
    // This Merge logic checks in delta table if there is already an existing record with the transformed df then update it ,
    // if it's new record then insert it, else if Operation is delete, it will delete from delta table
    // This provides us exactly once guarantee
    // source primaryKey which will be used get the latest records using group by
    val primaryKey = "id"
    val targetPath = sparkConfigUtil.getTargetPath.get.get

    val deltaTable = DeltaTable.forPath(targetPath)
    deltaTable
      .as("t")
      .merge(transformedDf.as("s"),
        s"s.$primaryKey = t.$primaryKey and t.dt >= current_date() - 3")
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute()
  }

}
