package p1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class WordCount {
    public static void main(String[] args) throws Exception {
        // set up the execution environment IDE 안에서 하면 로컬환경이고, 클러스터에 올려지면 remote 실행환경이다.
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        ParameterTool params = ParameterTool.fromArgs(args);

        // global 파라미터로 등록. 모든 노드에서 사용 가능
        env.getConfig().setGlobalJobParameters(params);

        /*
        여기까지 configuration
         */

        // read the text file from given input path (all the lines). 'String' 같은 특정 타입 선언 필요
        DataSet<String> text = env.readTextFile(params.get("input")); // [Noman Joyce Noman Sayuri Frances ...]

        // filter all the names starting with N
        DataSet<String> filtered = text.filter(new MyFilter()); // [Noman Nipun Noman ...]

        // cf) flat map : from single input -> multiple output
        DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer()); // [(Noman, 1), (Nipun, 1), (Noman, 1) ...]

        // groupby 0 : group it by first field of tuple (0 = first = 사람의 name)
        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(new int[] { 0 }).sum(1);

        // emit output
        if (params.has("output")) {
            counts.writeAsCsv(params.get("output"), "\n", " ");

            env.execute("WordCount Example");
        }
    }

    public static final class Tokenizer
            implements MapFunction<String, Tuple2<String, Integer>> {
        public Tuple2<String, Integer> map(String value) {
            return new Tuple2(value, Integer.valueOf(1));
        }
    }

    public static final class MyFilter implements FilterFunction<String> {
        // filter logic
        public boolean filter(String value) {
            return value.startsWith("N");
        }
    }
}
