package p1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Aggregation {

  private static final String INPUT_PATH = "/Users/mk-mac-281/Downloads/flink_tuto_lecture";
  private static final String OUTPUT_PATH = "/Users/mk-mac-281/Downloads/flink_tuto_output";

  public static void main(String[] args) throws Exception {
    // set up the streaming execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<String> data = env.readTextFile(INPUT_PATH + "/aggregation_flink_version_1.12/avg1");

    // month, category,product, profit
    DataStream<Tuple4<String, String, String, Integer>> mapped =
        data.map(new Splitter()); // tuple  [June,Category5,Bat,12]
    //       [June,Category4,Perfume,10]
    mapped.keyBy(t -> t.f0).sum(3).writeAsText(OUTPUT_PATH + "/out1");

    // 해당 필드만 min으로 업데이트. 이전 필드랑 섞이기 때문에
    mapped.keyBy(t -> t.f0).min(3).writeAsText(OUTPUT_PATH + "/out2");

    // track all fields 하려면 minBy를 쓴다
    mapped.keyBy(t -> t.f0).minBy(3).writeAsText(OUTPUT_PATH + "/out3");

    // 만약 object가 주어지면, field name 기준으로 적용한다. max("B") 이런식으로
    mapped.keyBy(t -> t.f0).max(3).writeAsText(OUTPUT_PATH + "/out4");

    mapped.keyBy(t -> t.f0).maxBy(3).writeAsText(OUTPUT_PATH + "/out5");
    // execute program
    env.execute("Aggregation");
  }

  // *************************************************************************
  // USER FUNCTIONS
  // *************************************************************************

  public static class Splitter
      implements MapFunction<String, Tuple4<String, String, String, Integer>> {
    public Tuple4<String, String, String, Integer> map(String value) {
      String[] words = value.split(",");
      return new Tuple4<>(words[1], words[2], words[3], Integer.parseInt(words[4]));
    }
  }
}