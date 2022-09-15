package org.example.datasource.postgres

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.sql.{DriverManager, ResultSet}
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


class DefaultSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = PostgresTable.schema

  override def getTable(
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String]
  ): Table = new PostgresTable(properties.get("tableName")) // TODO: Error handling
}

class PostgresTable(val name: String) extends SupportsRead with SupportsWrite {
  override def schema(): StructType = PostgresTable.schema

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE
  ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new PostgresScanBuilder(options)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new PostgresWriteBuilder(info.options)
}

object PostgresTable {
  val schema: StructType = new StructType().add("user_id", LongType)
}

case class ConnectionProperties(url: String, user: String, password: String, tableName: String,
                                partitionSize: Int , partitionColumn: String)

/** Read */

class PostgresScanBuilder(options: CaseInsensitiveStringMap) extends ScanBuilder {
  override def build(): Scan = new PostgresScan(ConnectionProperties(
    options.get("url"),
    options.get("user"),
    options.get("password"),
    options.get("tableName"),
    options.get("partitionSize").toInt,
    options.get("partitionColumn")
  ))
}

class PostgresPartition (
                        val url: String,
                        val user: String,
                        val password: String,
                        val tableName: String,
                        val limit: Int,
                        val offset: Int
                        )
  extends InputPartition

class PostgresScan(connect: ConnectionProperties) extends Scan with Batch {
  override def readSchema(): StructType = PostgresTable.schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = CustomPartitionList.makePartitionList(connect)

  override def createReaderFactory(): PartitionReaderFactory = new PostgresPartitionReaderFactory()

}

object CustomPartitionList {
  private def getBounds(props: ConnectionProperties): (Long, Long) = {
    val connection = DriverManager.getConnection(
      props.url, props.user, props.password)

    val statement = connection.createStatement()
    val query: String =
      s"""
     select
       min(${props.partitionColumn}) as min,
       max(${props.partitionColumn}) as max
       from ${props.tableName}"""
    val result = statement.executeQuery(query)
    result.next()
    (result.getInt(1), result.getInt(2))
  }

  def makePartitionList(props: ConnectionProperties): Array[InputPartition] = {
    val (low, upper) = getBounds(props)

    val partitionSize = props.partitionSize

    val partitions = ArrayBuffer[PostgresPartition]()

    for (value <- low .to(upper).by(partitionSize)) {
      val offset = value - 1

      partitions += new PostgresPartition(
        props.url,
        props.user,
        props.password,
        props.tableName,
        partitionSize.toInt,
        offset.toInt
      )
    }
    partitions.toArray
  }
}

class PostgresPartitionReaderFactory()
  extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow]
  = new PostgresPartitionReader(partition.asInstanceOf[PostgresPartition])
}

class PostgresPartitionReader(postgresPartition: PostgresPartition)
  extends PartitionReader[InternalRow] {
  private val connection = DriverManager.getConnection(
    postgresPartition.url, postgresPartition.user, postgresPartition.password
  )
  private val statement = connection.createStatement()
  private val resultSet = statement.executeQuery(
    s"""
        select *
                 from ${postgresPartition.tableName}
                 limit ${postgresPartition.limit}
                 offset ${postgresPartition.offset}
       """)

  override def next(): Boolean = resultSet.next()

  override def get(): InternalRow = InternalRow(resultSet.getLong(1))

  override def close(): Unit = connection.close()
}

/** Write */

class PostgresWriteBuilder(options: CaseInsensitiveStringMap) extends WriteBuilder {
  override def buildForBatch(): BatchWrite = new PostgresBatchWrite(ConnectionProperties(
    options.get("url"), options.get("user"), options.get("password"), options.get("tableName"),
    options.get("partitionSize").toInt, options.get("partitionColumn")
  ))
}

class PostgresBatchWrite(connectionProperties: ConnectionProperties) extends BatchWrite {
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory =
    new PostgresDataWriterFactory(connectionProperties)

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}
}

class PostgresDataWriterFactory(connectionProperties: ConnectionProperties) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId:Long): DataWriter[InternalRow] =
    new PostgresWriter(connectionProperties)
}

object WriteSucceeded extends WriterCommitMessage

class PostgresWriter(connectionProperties: ConnectionProperties) extends DataWriter[InternalRow] {

  val connection = DriverManager.getConnection(
    connectionProperties.url,
    connectionProperties.user,
    connectionProperties.password
  )
  val statement = "insert into users (user_id) values (?)"
  val preparedStatement = connection.prepareStatement(statement)

  override def write(record: InternalRow): Unit = {
    val value = record.getLong(0)
    preparedStatement.setLong(1, value)
    preparedStatement.executeUpdate()
  }

  override def commit(): WriterCommitMessage = WriteSucceeded

  override def abort(): Unit = {}

  override def close(): Unit = connection.close()
}

