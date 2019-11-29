package project;

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

public class JoinFlightsWithWeather extends Configured implements Tool {

  private static final Logger logger = LogManager.getLogger(JoinFlightsWithAirports.class);

  public static class OneBucketMapper extends Mapper<Object, Text, IntWritable, FlightOrGSOD> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    public void map(final Object key, final Text input, final Context context) throws IOException, InterruptedException {

    }

  }

  public static class FlightGSODReducer extends Reducer<NullWritable, FlightOrGSOD, NullWritable, Flight> {

    @Override
    public void reduce(final NullWritable key, final Iterable<FlightOrGSOD> values, final Context context) throws IOException, InterruptedException {

    }
  }

  @Override
  public int run(final String[] args) throws Exception {

    // Configuration
    final Configuration conf = getConf();
    final Job job = Job.getInstance(conf, "Flight-GSOD Join");
    job.setJarByClass(JoinWeatherWithStations.class);
    final Configuration jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", ",");

    // Classes for mapper, combiner and reducer
    job.setMapperClass(OneBucketMapper.class);
    job.setReducerClass(FlightGSODReducer.class);

    // Key and Value type for output
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Flight.class);

    // Path for input and output
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(final String[] args) {
    if (args.length != 3) {
      throw new Error("Three arguments required:\n<flight_file> <gsod_file> <outputDir>");
    }

    try {
      ToolRunner.run(new JoinFlightsWithWeather(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }
}
