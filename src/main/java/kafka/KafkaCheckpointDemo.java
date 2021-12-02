package kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaCheckpointDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "127.0.0.1:9092");

        KafkaSource<String> kafkaSource = createKafkaSource();
                //new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), p);
        // consumer.setStartFromLatest(); 얘가 있으면 플링크 꺼져있을 때 카프카로 들어간 메시지 못받는다

        //DataStream<String> kafkaData = env.addSource(consumer);
        DataStream<String> kafkaData = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "temp");
        env.enableCheckpointing(5000);

        DataStream<Tuple2<String, Integer>> counts =
                kafkaData.flatMap(new WordReader())
                        .keyBy(t -> t.f0)
                        .sum(1);
        counts.print();

        env.execute("Kafka Example New Job-20");
    }
    public static KafkaSource<String> createKafkaSource() {
        Properties p = new Properties();

        return KafkaSource.<String>builder()
                .setTopics("test")
                .setGroupId("only-consumer")
                .setBootstrapServers("127.0.0.1:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                //.setStartingOffsets(OffsetsInitializer.latest())
                //.setStartingOffsets(OffsetsInitializer.earliest())
                .setProperties(p)
                .build();
    }

    public static class WordReader implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }

    public static class UserParser implements FlatMapFunction<String, Tuple2<String, Integer>> {

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            ObjectMapper jsonParser = new ObjectMapper();
            JsonNode node = jsonParser.readValue(value, JsonNode.class);

            String username = node.get("name").asText();

            out.collect(new Tuple2<>(username, 1));
        }
    }
}
