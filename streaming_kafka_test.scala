import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.{Seconds, StateSpec, State}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{OffsetRange, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import scalikejdbc._



object streaming_kafka_test extends Logging {

  def initConPool(host:String, db:String, User:String, Passwd:String): ConnectionPool.type = {
      Class.forName("com.mysql.jdbc.Driver")    
      ConnectionPool.singleton(s"jdbc:mysql://${host}/${db}", User, Passwd)
      ConnectionPool 
  }


  def getSavedOff(pool: ConnectionPool.type, topic: String, group: String, tb: String): Map[TopicPartition, Long] = {
    // get offsets from db
    val table = SQLSyntax.createUnsafely(tb)
    using(DB(pool.borrow())) { db =>
      val offs = db.readOnly { implicit session =>
        sql"""select topic, _partition, _offset
              from ${table}
              where topic = ${topic} and custom_group = ${group}
        """
          .map { resultSet =>
            new TopicPartition(resultSet.string(1), resultSet.int(2)) -> resultSet.long(3)
          }.list.apply().toMap
      }
      logWarning(s"db saved offsets: ${offs}")
      offs
    }
  }


  def getTopicOff(broker:String, topic:String): Map[TopicPartition, Seq[Long]] = {
    // get topic offset ranges
    val props = new Properties()
    props.put("bootstrap.servers", broker)
    props.put("group.id", "offsetHunter")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val consumer = new KafkaConsumer(props)
    val postions = consumer
      .partitionsFor(topic)
      .asScala
      .map { t =>
        val tp = new TopicPartition(t.topic, t.partition)
        consumer.assign(Seq(tp).asJava)
        consumer.seekToEnd(Seq(tp).asJava)
        val lastest_offset = consumer.position(tp)
        consumer.seekToBeginning(Seq(tp).asJava)
        val earlist_offset = consumer.position(tp)
        tp -> Seq(earlist_offset, lastest_offset)
      }.toMap
    consumer.close()
    logWarning(s"kafka postions: ${postions}")
    postions
  }


  def mergeOff(offs:Map[TopicPartition, Long], postions:Map[TopicPartition, Seq[Long]]): Map[TopicPartition, Long] = {
    // merge offsets
    val fromOffsets = offs.isEmpty match {
      case true => postions.map { case (tp, Seq(s, l)) => tp -> s }
      case false =>
        (postions /: offs) { case (map, (k, v)) =>
          map + (k -> (v +: map.getOrElse(k, Nil)))
        } filter {
          case (_, o) => o.size != 1
        } map {
          case (tp, o) => tp -> o.sorted.dropRight(1).last
        }
    }
    logWarning(s"partitions and offsets: ${fromOffsets}")
    fromOffsets
  }


  def getOff(broker_server:String, topics:String, consumer:String, offset_table:String, pool:ConnectionPool.type): Map[TopicPartition, Long] = {
    // determine the offsets
    val saved_offsets = getSavedOff(pool, topics, consumer, offset_table)
    val kafka_postions = getTopicOff(broker_server, topics)
    mergeOff(saved_offsets, kafka_postions)
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
    val dbHost = "xxx"
    val dbName = "xxx"
    val dbUser = "xxx"
    val dbPasswd = "xxx"

    val spark = SparkSession
      .builder
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
    val fromOffsets = getOff(server, topic, consumer, table, mq_pool)
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
