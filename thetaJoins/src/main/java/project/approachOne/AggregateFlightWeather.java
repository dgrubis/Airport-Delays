package project.approachOne;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

import project.helperClasses.Flight;
import project.helperClasses.FlightOrGSOD;

public class AggregateFlightWeather extends Configured implements Tool {
  private static final Logger logger = LogManager.getLogger(AggregateFlightWeather.class);

  public static class FlightKeyMapper extends Mapper<Object, Text, NullWritable, Flight> {

    @Override
    public void map(final Object key, final Text input, final Context context)
            throws IOException, InterruptedException {


    }
  }

  public static class FlightAggregationReducer extends Reducer<IntWritable, FlightOrGSOD, NullWritable, Flight> {
    NullWritable nullKey = NullWritable.get();

    @Override
    protected void reduce(IntWritable key, Iterable<FlightOrGSOD> values, Context context)
            throws IOException, InterruptedException {


    }
  }

  @Override
  public int run(final String[] args) throws Exception {

    // Configuration
    final Configuration conf = getConf();
    final Job job = Job.getInstance(conf, "Aggregate Flights");
    job.setJarByClass(AggregateFlightWeather.class);
    final Configuration jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", ",");

    // Classes for mapper, combiner and reducer
    job.setMapperClass(FlightKeyMapper.class);
    job.setReducerClass(FlightAggregationReducer.class);

    // Key and Value type for output
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Flight.class);

    // Path for input and output
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(final String[] args) {
    if (args.length != 2) {
      throw new Error("Two arguments required: <inputDir> <outputDir>");
    }

    try {
      ToolRunner.run(new AggregateFlightWeather(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }

}
