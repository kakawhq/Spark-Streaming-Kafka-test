import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StateSpec, State}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import kafka.utils.ZkUtils
import org.apache.spark.streaming.kafka010.{OffsetRange, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import scalikejdbc._



object streaming_kafka_test {

  def initConPool(host:String, db:String, User:String, Passwd:String): ConnectionPool.type = {
      Class.forName("com.mysql.jdbc.Driver")    
      ConnectionPool.singleton(s"jdbc:mysql://${host}/${db}", User, Passwd)
      ConnectionPool 
  }


  def getCommittedOff(pool:ConnectionPool.type, TOPIC_NAME:String, GROUP_ID:String, TableName:String, zkQuorum:String, zkRootDir:String): Map[TopicPartition,Long] = {
    //get offset in db
    val table = SQLSyntax.createUnsafely(TableName)
    using(DB(pool.borrow())) { db =>
      val Off = db.readOnly { implicit session =>      
        sql"""select topic, _partition, _offset
              from ${table}
              where topic = ${TOPIC_NAME} and custom_group = ${GROUP_ID}
        """
          .map { resultSet =>          
            new TopicPartition(resultSet.string(1), resultSet.int(2)) -> resultSet.long(3)        
          }.list.apply().toMap
      }
      val zkUrl = s"${zkQuorum}/${zkRootDir}"
      val zkClientAndConnection = ZkUtils.createZkClientAndConnection(zkUrl, 10000, 10000)
      val zkUtils = new ZkUtils(zkClientAndConnection._1, zkClientAndConnection._2, false)
      val numPartition = zkUtils.getPartitionsForTopics(Seq(TOPIC_NAME)).get(TOPIC_NAME).toList.head.size
      var fromOffsets = Map[TopicPartition,Long]()
      if (Off.size == 0) {
        for (i <- 0 until numPartition) {
          fromOffsets += (new TopicPartition(TOPIC_NAME, i) -> 0L)
        }
        println("init", fromOffsets)
      } else if(numPartition > Off.size) {
        fromOffsets = Off
        for (i <- Off.size until numPartition) {   
          fromOffsets += (new TopicPartition(TOPIC_NAME, i) -> 0L)
        }
        println("add partitions", fromOffsets)
      } else {
        fromOffsets = Off
        println("partitions no change", fromOffsets)
      }
      fromOffsets
    }
  }


  def saveOff(pool:ConnectionPool.type, GROUP_ID:String, offsetRanges:Array[OffsetRange], TableName:String) {
    //save offset to db
    val table = SQLSyntax.createUnsafely(TableName)
    using(DB(pool.borrow())) { db =>
      db.localTx { implicit session =>        
        offsetRanges.foreach { offsetRange =>          
          val offsetRows =            
            sql"""REPLACE INTO ${table}
                    (topic,_partition,custom_group,_offset)
                  VALUES  
                    (${offsetRange.topic},
                    ${offsetRange.partition},
                    ${GROUP_ID},
                    ${offsetRange.untilOffset})             
                """.update.apply()
        }
      }
    }
  }


  def main(args: Array[String]): Unit = {
    
    val server = "broker1:9092,broker2:9092"
    val customer_group = "customer1"
    val topics = "test1"
    val table = "kafka_offset"
    val zkQuorum = "broker1:2181"
    val zkKafkaRootDir = ""
    val dbHost = "xxx"
    val dbName = "xxx"
    val dbUser = "xxx"
    val dbPasswd = "xxx"

    val spark = SparkSession.builder
                            .appName("test")
                            .config("spark.streaming.stopGracefullyOnShutdown", "true")
                            .config("spark.streaming.kafka.maxRatePerPartition", "10000")
                            .getOrCreate()
                            
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(30))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> server,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> customer_group,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val pool = initConPool(dbHost, dbName, dbUser, dbPasswd)
    val fromOffsets = getCommittedOff(pool, topics, customer_group, table, zkQuorum, zkKafkaRootDir)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Assign[String, String](fromOffsets.keys, kafkaParams, fromOffsets)
    )

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      if (!rdd.isEmpty) {
        rdd.collect().foreach(println)
        offsetRanges.foreach(offset => println(offset.topic, offset.partition, offset.fromOffset, offset.untilOffset))
        saveOff(pool, customer_group, offsetRanges, table)
      } else {
        println("no data this batch !!!")
      }
      println("=============================")
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
