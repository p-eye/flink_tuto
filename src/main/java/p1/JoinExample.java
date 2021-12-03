package p1;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

@SuppressWarnings("serial")
public class JoinExample {
  public static void main(String[] args) throws Exception {

    // set up the execution environment
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    final ParameterTool params = ParameterTool.fromArgs(args);
    // make parameters available in the web interface
    env.getConfig().setGlobalJobParameters(params);

    // Read person file and generate tuples out of each string read
    DataSet<Tuple2<Integer, String>> personSet = env.readTextFile(params.get("input1"))
        .map(new MyMap());  // tuple of (1 John)

    // Read location file and generate tuples out of each string read
    DataSet<Tuple2<Integer, String>> locationSet = env.readTextFile(params.get("input2"))
            .map(new MyMap());   // tuple of (1, DC)

    // join datasets on person_id   // where person 0 field == location 0 field - 여기까지 결과 : (1, John), (1, DC) \n ...
    // with 이하 : optional (joined format will be <id, person_name, state>)
    DataSet<Tuple3<Integer, String, String>> joined =
        personSet.fullOuterJoin(locationSet).where(0).equalTo(0)
            .with(new MyFullOuterJoin());

    joined.writeAsCsv(params.get("output"), "\n", " ");

    env.execute("Join example");
  }

  public static final class MyMap implements MapFunction<String, Tuple2<Integer, String>> {
    // map logic
    public Tuple2<Integer, String> map(String value) {
      String[] words = value.split(",");      // words = [ {1}, {John}]
      return new Tuple2<>(Integer.parseInt(words[0]), words[1]);
    }
  }

  public static final class MyInnerJoin implements
      JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> {
    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person,
                                                Tuple2<Integer, String> location) {
      return new Tuple3<>(person.f0, person.f1,
          location.f1);         // returns tuple of (1 John DC)
    }
  }

  public static final class MyLeftOuterJoin implements
      JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> {
    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person,
                                                Tuple2<Integer, String> location) {
      if (location == null) {
        return new Tuple3<>(person.f0, person.f1, "NULL");
      }
      return new Tuple3<>(person.f0, person.f1,
          location.f1);         // returns tuple of (1 John DC)
    }
  }

  public static final class MyRightOuterJoin implements
      JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> {
    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person,
                                                Tuple2<Integer, String> location) {
      if (person == null) {
        return new Tuple3<>(location.f0, "NULL", location.f1);
      }
      return new Tuple3<>(person.f0, person.f1,
          location.f1);         // returns tuple of (1 John DC)
    }
  }

  public static final class MyFullOuterJoin implements
      JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> {
    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person,
                                                Tuple2<Integer, String> location) {
      if (location == null) {
        return new Tuple3<>(person.f0, person.f1, "NULL");
      } else if (person == null) {
        return new Tuple3<>(location.f0, "NULL", location.f1);
      }
      return new Tuple3<>(person.f0, person.f1, location.f1);
    }
  }
}
