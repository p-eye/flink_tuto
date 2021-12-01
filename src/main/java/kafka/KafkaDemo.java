package kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = createKafkaSource();
        DataStream<String> kafkaData = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "temp");
        DataStream<Tuple2<String, Integer>> counts = kafkaData.flatMap(new WordReader())
                        .keyBy(t -> t.f0)
                        .sum(1);
        counts.print();

        env.execute("Kafka Example");
    }

    public static KafkaSource<String> createKafkaSource() {
        Properties p = new Properties();

        return KafkaSource.<String>builder()
                .setTopics("test")
                .setBootstrapServers("127.0.0.1:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(p)
                .build();
    }

    public static class WordReader implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] words = value.split(" ");
            for (String word: words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}