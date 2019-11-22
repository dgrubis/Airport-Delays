package project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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

import project.helperClasses.GSOD;
import project.helperClasses.LatLong;

/**
 * Preprocessing job to equi-join weather stations data with weather observations data, producing a
 * dataset of weather observations that includes the latitude and longitude of the observing weather
 * station.  Replicated join algorithm is used due to the small size of the weather station data.
 */
public class JoinWeatherWithStations extends Configured implements Tool {
  private static final Logger logger = LogManager.getLogger(JoinWeatherWithStations.class);
  private static final String FILE_LABEL = "fileLabel";

  public static class repJoinMapper extends Mapper<Object, Text, NullWritable, GSOD> {
    // Using HashMap instead of MultiMap because each key should identify one station only
    private Map<String, LatLong> weatherStationsMap = new HashMap<>();
    private NullWritable nullKey = NullWritable.get();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      BufferedReader reader = getReaderFromFileCache(context);
      String record;
      while ((record = reader.readLine()) != null) {
        String[] station = record.split(",");
        String USAF_WBAN = station[0] + station[1];
        LatLong location = new LatLong(station[7], station[8]);
        weatherStationsMap.put(USAF_WBAN, location);
      }
      reader.close();
    }

    private BufferedReader getReaderFromFileCache(Context context) throws IOException, RuntimeException {
      URI[] cacheFiles = context.getCacheFiles();
      if (cacheFiles == null || cacheFiles.length == 0) {
        throw new RuntimeException("Input file was not added to the Distributed File Cache.");
      }
      return new BufferedReader(new FileReader(FILE_LABEL));
    }

    @Override
    public void map(final Object key, final Text input, final Context context) throws IOException, InterruptedException {
      GSOD observation = GSOD.parseCSV(input.toString());
      LatLong location = weatherStationsMap.get(observation.getUSAF_WBAN());
      if (location == null){
        logger.info("Observation did not match station with USAF_WBAN = " + observation.getUSAF_WBAN());
        return;
      }
      observation.setLocation(location);
      context.write(nullKey, observation);

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
    job.addCacheFile(new URI(args[1] + "#" + FILE_LABEL));

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