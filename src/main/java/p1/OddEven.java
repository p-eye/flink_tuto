package p1;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class OddEven {

    private static final String INPUT_PATH = "/Users/mk-mac-281/Downloads/flink_tuto_lecture";
    private static final String OUTPUT_PATH = "/Users/mk-mac-281/Downloads/flink_tuto_output";

    public static void main(String[] args) throws Exception {
        // set up the stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> text = env.readTextFile(INPUT_PATH + "/split operator_flink_version_1.12/oddeven");

        // String type side output for Even values
        final OutputTag<String> evenOutTag = new OutputTag<>("even-string-output") {};
        // Integer type side output for Odd values
        final OutputTag<Integer> oddOutTag = new OutputTag<>("odd-int-output") {};

        SingleOutputStreamOperator<Integer> mainStream = text
                .process(new ProcessFunction<> () {
                    @Override
                    public void processElement(String value, Context ctx, Collector<Integer> out) throws Exception {

                        int intVal = Integer.parseInt(value);
                        // get all data in regular output as well
                        out.collect(intVal);

                        if (intVal % 2 == 0) {
                            // emit data to side output for even output
                            ctx.output(evenOutTag, String.valueOf(intVal));
                        } else {
                            // emit data to side output for even output
                            ctx.output(oddOutTag, intVal);
                        }
                    }
                });

        DataStream<String> evenSideOutputStream = mainStream.getSideOutput(evenOutTag);
        DataStream<Integer> oddSideOutputStream = mainStream.getSideOutput(oddOutTag);

        evenSideOutputStream.writeAsText(OUTPUT_PATH + "/even");
        oddSideOutputStream.writeAsText(OUTPUT_PATH + "/odd");

        // execute program
        env.execute("ODD EVEN");
    }
}