package jdbc;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JdbcDemo {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.fromElements(
            new User(7, "user7"),
            new User(8, "user8"),
            new User(9, "user9"))
        .addSink(JdbcSink.sink(
            "insert into user_test values(?, ?)",
            (statement, user) -> {
              statement.setInt(1, user.id);
              statement.setString(2, user.name);
            },
            createJdbcExecOpts(),
            createJdbcConnection()));

    env.execute();
  }

  private static JdbcConnectionOptions createJdbcConnection() {
    return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:postgresql://localhost:5432/flink_db")
        .withDriverName("org.postgresql.Driver")
        .withUsername("flink")
        .withPassword("1234")
        .build();
  }

  private static JdbcExecutionOptions createJdbcExecOpts() {
    return JdbcExecutionOptions.builder()
        .withBatchSize(1000)
        .withBatchIntervalMs(200)
        .withMaxRetries(5)
        .build();
  }
}
