package project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import project.helperClasses.USAF_WBAN;

/**
 * Preprocessing job to equi-join weather stations data with weather observations data, producing a
 * dataset of weather observations that includes the latitude and longitude of the observing weather
 * station.  Replicated join algorithm is used due to the small size of the weather station data.
 */
public class JoinWeatherWithStations extends Configured implements Tool {
  private static final Logger logger = LogManager.getLogger(JoinWeatherWithStations.class);

  public static class repJoinMapper extends Mapper<Object, Text, Text, IntWritable> {
    // Using HashMap instead of MultiMap because each key should identify one station only
    private Map<String, String> weatherStationsMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      BufferedReader reader = getReaderFromFileCache(context);
      String record;
      while ((record = reader.readLine()) != null) {
        USAF_WBAN stationKey = USAF_WBAN.parseFromStationCSV(record);
        //followerUserHashMap.put(followerUserPair.getFirst(), followerUserPair.getSecond());
      }
      reader.close();
    }

    private BufferedReader getReaderFromFileCache(Context context) throws IOException, RuntimeException {
      URI[] cacheFiles = context.getCacheFiles();
      if (cacheFiles == null || cacheFiles.length == 0) {
        throw new RuntimeException("Input file was not added to the Distributed File Cache.");
      }
      return new BufferedReader(new FileReader("filelabel"));
    }

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

    }
  }


  @Override
  public int run(final String[] args) throws Exception {

    // Configuration
    final Configuration conf = getConf();
    final Job job = Job.getInstance(conf, "Weather Join");
    job.setJarByClass(JoinWeatherWithStations.class);
    final Configuration jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", ",");

    // Classes for mapper, combiner and reducer
    job.setMapperClass(repJoinMapper.class);
    job.setNumReduceTasks(0);

    // Key and Value type for output
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // Path for input and output
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    // Set up distributed cache with a copy of weather station data
    //TODO: make filelabel a constant value
    job.addCacheFile(new URI(args[1] + "#filelabel"));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(final String[] args) {
    if (args.length != 3) {
      throw new Error("Three arguments required:\n<inputFileToBeRead> <inputFileToBeBroadcast> <outputDir>");
    }

    try {
      ToolRunner.run(new JoinWeatherWithStations(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }

}