package es.roberveral.dataflow.io;

import com.cloudera.dataflow.spark.SparkPipelineOptions;
import com.cloudera.dataflow.spark.SparkPipelineRunner;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;

public class JDBCIOTest {
    public static interface TestJDBCOptions extends PipelineOptions, SparkPipelineOptions {

        @Description("Path of the file to write to")
        @Default.String("gs://<YOUR-BUCKET>/output")
        String getOutput();
        void setOutput(String value);

        @Description("Runner to execute in")
        @Default.String("DataflowRunner")
        String getRunnerName();
        void setRunnerName(String value);
    }

    /** A SimpleFunction that converts a Word and Count into a printable string. */
    public static class FormatAsTextFn extends SimpleFunction<JdbcIO.Record, String> {
        private static final long serialVersionUID = 1L;

        public String apply(JdbcIO.Record input) {
            return input.toString();
        }
    }

    public static void main(String[] args) throws Exception {
        TestJDBCOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(TestJDBCOptions.class);
        if (options.getRunnerName().equals("SparkRunner")) {
            options.setRunner(SparkPipelineRunner.class);
        } else if (options.getRunnerName().equals("DataflowRunner")) {
            options.setRunner(BlockingDataflowPipelineRunner.class);
        } else if (options.getRunnerName().equals("DirectRunner")) {
            options.setRunner(DirectPipelineRunner.class);
        }
        Pipeline p = Pipeline.create(options);
        p.apply(Read.from(JdbcIO.source("jdbc:mysql://quickstart:3306/retail_db", "com.mysql.jdbc.Driver", "retail_dba", "cloudera", "SELECT * FROM orders")))
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply(TextIO.Write.named("WriteData").to(options.getOutput()));
        p.run();
    }
}
