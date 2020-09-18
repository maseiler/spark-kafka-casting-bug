import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadLongFromKafka {

    private static Logger LOG = LoggerFactory.getLogger(ReadLongFromKafka.class);
    private SparkSession spark;

    public ReadLongFromKafka() {
        createSparkSession();

        Dataset<Row> input = createKafkaConsumer();
        //Dataset<Row> query = input.select(functions.col("key"), functions.col("value"), functions.col("timestamp"));

        // TODO: the following casts are *not* working
        //Dataset<Row> query = input.selectExpr("BIGINT(key)", "value", "timestamp");
        //Dataset<Row> query = input.selectExpr("LONG(key)", "value", "timestamp");
        //Dataset<Row> query = input.selectExpr("CAST(key AS BIGINT)", "value", "timestamp");
        //Dataset<Row> query = input.selectExpr("CAST(key AS LONG)", "value", "timestamp");
        //Dataset<Row> query = input.select(functions.col("key").cast("LONG"), functions.col("value"), functions.col("timestamp"));
        Dataset<Row> query = input.select(functions.col("key").cast(DataTypes.LongType), functions.col("value"), functions.col("timestamp"));

        LOG.info("######### SCHEMA ######### ");
        query.printSchema();

        DataStreamWriter<Row> dataStream = query.writeStream()
                .format("console")
                .trigger(Trigger.ProcessingTime("3 seconds"));

        try {
            dataStream.start().awaitTermination();
        } catch (Exception e) {
            LOG.error("Unhandled Exception: ", e);
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        new ReadLongFromKafka();
    }

    private void createSparkSession() {
        spark = SparkSession
                .builder()
                .appName("ReadLongFromKafka")
                .master("local")
                .getOrCreate();
    }

    private Dataset<Row> createKafkaConsumer() {
        return spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "127.0.0.1:9092")
                .option("subscribe", "spark-long-test")
                .option("startingOffsets", "latest")
                .load();
    }
}
