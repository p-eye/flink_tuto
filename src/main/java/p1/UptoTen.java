package p1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UptoTen {

    private static final String OUTPUT_PATH = "/Users/mk-mac-281/Downloads/flink_tuto_output";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Long, Integer>> data = env.generateSequence(0, 5).map(new MapFunction<>() {
            public Tuple2<Long, Integer> map(Long value) {
                return new Tuple2<>(value, 0); // 아직 0번의 iteration
            }
        });

        // prepare stream for iteration
        IterativeStream<Tuple2<Long, Integer>> iteration = data.iterate(5000);   // ( 0,0   1,0  2,0  3,0   4,0  5,0 )

        // define iteration
        DataStream<Tuple2<Long, Integer>> plusOne = iteration.map(new MapFunction<>() {
            public Tuple2<Long, Integer> map(Tuple2<Long, Integer> value) {
                if (value.f0 == 10)
                    return value;
                else
                    return new Tuple2<>(value.f0 + 1, value.f1 + 1);
            }
        });   //   plusone  (1,1   2,1  3,1   4,1   5,1   6,1) , (2,2   3,2  4,2   5,2   6,2   7,2)

        // part of stream to be used in next iteration (
        DataStream<Tuple2<Long, Integer>> notEqualToTen = plusOne.filter(new FilterFunction<>() {
            public boolean filter(Tuple2<Long, Integer> value) {
                return value.f0 != 10;
            }
        });

        // feed data back to next iteration
        iteration.closeWith(notEqualToTen);

        // data not feedback to iteration
        DataStream<Tuple2<Long, Integer>> equalToTen = plusOne.filter(
                (FilterFunction<Tuple2<Long, Integer>>) value -> value.f0 == 10);

        equalToTen.writeAsText(OUTPUT_PATH + "/ten");

        env.execute("Iteration Demo");
    }
}

