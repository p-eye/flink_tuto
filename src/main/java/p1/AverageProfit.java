package p1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AverageProfit {

  public static void main(String[] args) throws Exception {

    // set up the streaming execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<String> data = env.readTextFile(
        "/Users/mk-mac-281/Downloads/flink_tuto_lecture/reduce operator_flink_version_1.12/avg");

    // month, product, category, profit, count ([June,Category5,Bat,12,1] [June,Category4,Perfume,10,1])
    DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = data.map(new Splitter());

    // groupBy 'month' (June { [Category5,Bat,12,1] [Category4,Perfume,10,1] })
    // reduced = { [Category4,Perfume,22,2] ..... } rolling
    DataStream<Tuple5<String, String, String, Integer, Integer>> reduced =
        mapped.keyBy(t -> t.f0).reduce(new Reduce1());

    // month, avg. profit
    DataStream<Tuple2<String, Double>> profitPerMonth = reduced.map(new MyMap());

    //profitPerMonth.print();
    profitPerMonth.writeAsText(
        "/Users/mk-mac-281/Downloads/flink_tuto_output/profit_per_month.txt");

    // execute program
    env.execute("Avg Profit Per Month");
  }

  // *************************************************************************
  // USER FUNCTIONS
  // *************************************************************************

  public static class Reduce1
      implements ReduceFunction<Tuple5<String, String, String, Integer, Integer>> {
    /*
    월별 평균 이익 구하기
    월별로 result = f(pre, curr)
    숫자와 관련없는 카테고리, 상품명 등은 current로 덮으면서 profit sum, count는 누적 합
     */
    public Tuple5<String, String, String, Integer, Integer> reduce(
        Tuple5<String, String, String, Integer, Integer> current,
        Tuple5<String, String, String, Integer, Integer> pre_result) {
      return new Tuple5<>(current.f0, current.f1, current.f2, current.f3 + pre_result.f3,
          current.f4 + pre_result.f4);
    }
  }

  public static class Splitter
      implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>> {
    // 01-06-2018,June,Category5,Bat,12
    public Tuple5<String, String, String, Integer, Integer> map(String value) {
      String[] words = value.split(","); // words = [{01-06-2018},{June},{Category5},{Bat}.{12}
      // ignore timestamp, we don't need it for any calculations (June    Category5   Bat     12)
      return new Tuple5<>(words[1], words[2], words[3], Integer.parseInt(words[4]), 1);
    }
  }

  public static class MyMap implements
      MapFunction<Tuple5<String, String, String, Integer, Integer>, Tuple2<String, Double>> {
    /*
    카테고리, 상품명 필터하고 month와 average profit만 남김
     */
    public Tuple2<String, Double> map(Tuple5<String, String, String, Integer, Integer> input) {
      return new Tuple2<>(input.f0, (input.f3 * 1.0) / input.f4);
    }
  }
}