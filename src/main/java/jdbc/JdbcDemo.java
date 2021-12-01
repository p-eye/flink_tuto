package jdbc;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JdbcDemo {
    static class User {
        public User(int id, String name) {
            this.id = id;
            this.name = name;
        }
        final int id;
        final String name;
    }

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(
                new User(4, "user4"),
                new User(5, "user5"),
                new User(6, "user6"))
            .addSink(JdbcSink.sink(
                        "insert into user_test values(?, ?)",
                        (statement, user) -> {
        statement.setInt(1, user.id);
        statement.setString(2, user.name);
    },
            JdbcExecutionOptions.builder()
            .withBatchSize(1000)
                                .withBatchIntervalMs(200)
                                .withMaxRetries(5)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:postgresql://localhost:5432/flink_db")
                                .withDriverName("org.postgresql.Driver")
                                .withUsername("flink")
                                .withPassword("1234")
                                .build()
                ));

        env.execute();

    }

}
