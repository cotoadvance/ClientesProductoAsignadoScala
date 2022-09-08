package local.bigdata.tchile.clientes_producto_asignado

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import java.time.format.DateTimeFormatter

object proceso {
  def main(args: Array[String]): Unit = {
    // ---------------------------------------------------------------
    // INICIO PARAMETRIZACION
    // ---------------------------------------------------------------

    // BASE

    val conf = new SparkConf().setMaster("yarn").setAppName("B2B").set("spark.driver.allowMultipleContexts", "true").set("spark.yarn.queue", "default").set("spark.sql.crossJoin.enabled", "true").set("spark.sql.debug.maxToStringFields", "1000").set("spark.sql.autoBroadcastJoinThreshold", "-1")
    val spark: SparkSession = SparkSession.builder().config(conf).config("spark.sql.warehouse.dir", "/usr/hdp/current/spark-client/conf/hive-site.xml").config("spark.submit.deployMode", "cluster").enableHiveSupport().getOrCreate()
    import spark.implicits._

    // ---------------------------------------------------------------

    // PARAMETROS

    val currentDate = java.time.LocalDateTime.now
    val currentYear = currentDate.getYear.toString
    val currentMonth = currentDate.getMonthValue.toString.reverse.padTo(2, '0').reverse
    val currentDay = currentDate.getDayOfMonth.toString.reverse.padTo(2, '0').reverse

    val ASSIGNED_PRODUCT_STATUS_DESC = "Activo"

    // ACCESO EXADATA

    val exaDriver = "oracle.jdbc.OracleDriver"
    val exaUrl = "jdbc:oracle:thin:@(DESCRIPTION =(ENABLE=BROKEN)(ADDRESS=(PROTOCOL=TCP)(HOST=smt-scan.tchile.local)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=EXPLOTA)))"
    val exaUser = args(0)
    val exaPass = args(1)

    // DESTINO EXADATA

    val schema = "PRODUCTO_ASIGNADO"
    val table = "PRODUCTO_ASIGNADO_B2B"

    // PATHS

    val conformadoBasePath = "/modelos/producto_asignado/producto_asignado_b2b/conformado"
    val conformadoFullPath = s"$conformadoBasePath/year=$currentYear/month=$currentMonth/day=$currentDay"
    val assignedProductPath = "/modelos/ods/assigned_product/conformado"
    val productPath = "/modelos/ods/product/conformado"
    val productCatalogTypePath = "/modelos/ods/product_catalog_type/conformado/conformado.odschile.product_catalog_type.f.d.csv"
    val assignedProductStatePath = "/modelos/ods/assigned_product_state/conformado"
    val assignedProductStatusPath = "/modelos/ods/assigned_product_status/conformado"
    val customerPath = "/modelos/ods/customer/conformado"
    val contractRutPath = "/modelos/ods/contract_rut/conformado"
    val parqueSistPath = "/data/clientes/segmento_empresas/b2b/sii/parque_sist/archive/20220208"

    // SCHEMAS

    val productCatalogTypeSchema = StructType(Array(
      StructField("product_catalog_type_key", StringType, nullable = true),
      StructField("product_catalog_type_desc", StringType, nullable = true),
      StructField("product_catalog_type_id", StringType, nullable = true),
      StructField("source_system_id", StringType, nullable = true),
      StructField("source_timestamp", StringType, nullable = true),
      StructField("etl_crd_dt", StringType, nullable = true),
      StructField("etl_upd_dt", StringType, nullable = true),
      StructField("batch_id", StringType, nullable = true),
      StructField("dml_ind", StringType, nullable = true),
      StructField("bigdata_close_date", StringType, nullable = true),
      StructField("bigdata_ctrl_id", StringType, nullable = true)
    ))

    // ---------------------------------------------------------------
    // FIN PARAMETRIZACION
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // INICIO PROCESO
    // ---------------------------------------------------------------

    val assignedProductStatus = spark.read.parquet(assignedProductStatusPath).
      filter($"ASSIGNED_PRODUCT_STATUS_DESC" === ASSIGNED_PRODUCT_STATUS_DESC).
      select("ASSIGNED_PRODUCT_STATUS_KEY")
    val assignedProduct = spark.read.parquet(assignedProductPath).
      withColumn("SUSCRIPTOR", $"SUBSCRIBER_KEY").
      withColumn("FECHA_INICIO_PRODUCTO", $"START_DATE".cast("date")).
      withColumn("FECHA_FIN_PRODUCTO", $"END_DATE".cast("date")).
      filter(
        $"FECHA_FIN_PRODUCTO" > currentDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
        and $"ASSIGNED_PRODUCT_STATUS_KEY" === assignedProductStatus.select($"ASSIGNED_PRODUCT_STATUS_KEY").first.getInt(0)
      ).
      select(
        "ASSIGNED_PRODUCT_KEY",
        "CUSTOMER_KEY",
        "SUSCRIPTOR",
        "PRIMARY_RESOURCE_VALUE",
        "ORDER_MODE_IND",
        "FECHA_INICIO_PRODUCTO",
        "FECHA_FIN_PRODUCTO",
        "SOURCE_SYSTEM_ID",
        "AP_VERSION_ID",
        "PRODUCT_CATALOG_KEY",
        "PRODUCT_KEY",
        "ASSIGNED_PRODUCT_ID",
        "PARENT_ASSIGEN_PRODUCT_KEY",
        "PRODUCT_OFFER_ID",
        "ADD_ON_OFFER_DEF_ID",
        "AP_ID_DOMINANT",
        "SUB_MAIN_OFFER_KEY",
        "ASSIGNED_PRODUCT_STATE_KEY",
        "ASSIGNED_PRODUCT_STATUS_KEY",
        "MODIFICATION_USER",
        "BSS_CREATION_DATE"
      )
    val product = spark.read.parquet(productPath).
      withColumn("DESCRIPCION_PRODUCTO", $"PRODUCT_DESC").
      select("PRODUCT_CATALOG_TYPE_KEY", "DESCRIPCION_PRODUCTO", "PRODUCT_KEY")
    val productCatalogType = spark.read.
      schema(productCatalogTypeSchema).
      option("delimiter", "~").
      option("header", "false").
      csv(productCatalogTypePath).
      filter($"dml_ind" =!= "DELETE").
      select("PRODUCT_CATALOG_TYPE_ID", "PRODUCT_CATALOG_TYPE_DESC", "dml_ind", "PRODUCT_CATALOG_TYPE_KEY")
    val assignedProductState = spark.read.parquet(assignedProductStatePath).
      withColumn("ESTADO_PRODUCTO", $"ASSIGNED_PRODUCT_STATE_DESC").
      select("ESTADO_PRODUCTO", "ASSIGNED_PRODUCT_STATE_ID", "ASSIGNED_PRODUCT_STATE_KEY")
    val customer = spark.read.parquet(customerPath).
      select("CUSTOMER_KEY", "CONTRACT_RUT_KEY")
    val contractRut = spark.read.parquet(contractRutPath).
      select("CONTRACT_RUT_KEY", "CONTRACT_RUT_DESC")
    val parqueSist = spark.read.orc(parqueSistPath).
      withColumn("RUT_CLIENTE", trim($"rut")).
      withColumn("NOMBRE_CLIENTE", trim($"razon_social")).
      withColumn("CODIGO_CLIENTE", $"CODIGO_CLIENTE".cast("long")).
      withColumn("SUSCRIPTOR", $"subcriptor".cast("long")).
      withColumn("NUM_CELULAR", trim($"numero")).
      withColumn("ESTADO_LINEA", trim($"estado")).
      withColumn("FECHA_ALTA", to_date($"FECHA_ALTA", "yyyyMMdd")).
      withColumn("DESCRIPCION_PLAN", trim($"descripcion")).
      select("RUT_CLIENTE", "NOMBRE_CLIENTE", "CODIGO_CLIENTE", "SEGMENTO", "SUSCRIPTOR", "NUM_CELULAR", "ESTADO_LINEA", "FECHA_ALTA", "IMEI", "SIMCARD", "DESCRIPCION_PLAN", "TIPO_PLAN").
      filter(!$"ESTADO_LINEA".like("Cancell%"))

    val conformado = parqueSist.
      join(assignedProduct, Seq("SUSCRIPTOR"), "left").
      join(product, Seq("PRODUCT_KEY")).
      join(productCatalogType, Seq("PRODUCT_CATALOG_TYPE_KEY")).
      join(assignedProductState, Seq("ASSIGNED_PRODUCT_STATE_KEY")).
      join(customer, Seq("CUSTOMER_KEY"), "left").
      join(contractRut, Seq("CONTRACT_RUT_KEY"), "left").
      withColumn("STATUS_PRODUCTO", lit(ASSIGNED_PRODUCT_STATUS_DESC)).
      withColumn("FECHA_PROCESO", lit(java.time.LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))).cast("date")).
      withColumn("CODIGO_PLAN", $"PRODUCT_KEY").
      select(
        "RUT_CLIENTE",
        "NOMBRE_CLIENTE",
        "CODIGO_CLIENTE",
        "CODIGO_PLAN",
        "SEGMENTO",
        "SUSCRIPTOR",
        "NUM_CELULAR",
        "ESTADO_LINEA",
        "FECHA_ALTA",
        "IMEI",
        "SIMCARD",
        "DESCRIPCION_PLAN",
        "TIPO_PLAN",
        "DESCRIPCION_PRODUCTO",
        "ESTADO_PRODUCTO",
        "STATUS_PRODUCTO",
        "FECHA_INICIO_PRODUCTO",
        "FECHA_FIN_PRODUCTO",
        "FECHA_PROCESO"
      ).cache()

    conformado.repartition(1).write.mode("overwrite").parquet(conformadoFullPath)

    // ---------------------------------------------------------------
    // FIN PROCESO
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // INICIO ESCRITURA HIVE
    // ---------------------------------------------------------------

    spark.sql(s"DROP TABLE IF EXISTS $schema.$table")

    spark.sql(s"""CREATE EXTERNAL TABLE $schema.$table(
                 |RUT_CLIENTE string
                 |,NOMBRE_CLIENTE string
                 |,CODIGO_CLIENTE long
                 |,CODIGO_PLAN int
                 |,SEGMENTO string
                 |,SUSCRIPTOR long
                 |,NUM_CELULAR string
                 |,ESTADO_LINEA string
                 |,FECHA_ALTA date
                 |,IMEI string
                 |,SIMCARD string
                 |,DESCRIPCION_PLAN string
                 |,TIPO_PLAN string
                 |,DESCRIPCION_PRODUCTO string
                 |,ESTADO_PRODUCTO string
                 |,STATUS_PRODUCTO string
                 |,FECHA_INICIO_PRODUCTO date
                 |,FECHA_FIN_PRODUCTO date
                 |,FECHA_PROCESO date
                 |)
                 |PARTITIONED BY
                 |(year string, month string, day string)
                 |STORED AS PARQUET
                 |LOCATION '$conformadoBasePath'""".stripMargin)

    spark.sql(s"MSCK REPAIR TABLE $schema.$table")

    // ---------------------------------------------------------------
    // FIN ESCRITURA HIVE
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // INICIO ESCRITURA EXADATA
    // ---------------------------------------------------------------

    val doTheTruncate = s"CALL $schema.DO_THE_TRUNCATE('$table')"

    spark.read.format("jdbc").option("url", exaUrl).option("driver", exaDriver).option("sessionInitStatement", doTheTruncate).option("dbtable", s"$schema.$table").option("user", exaUser).option("password", exaPass).option("numPartitions", 1).load().show(1)

    conformado.write.format("jdbc").option("numPartitions", 1).option("url", exaUrl).option("dbtable", s"$schema.$table").option("user", exaUser).option("password", exaPass).option("driver", exaDriver).mode("append").save()

    // ---------------------------------------------------------------
    // END ESCRITURA EXADATA
    // ---------------------------------------------------------------

    spark.close()
  }
}