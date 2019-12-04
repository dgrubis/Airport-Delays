package project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import project.helperClasses.LatLon;
import project.helperClasses.WeatherStation;
import project.helperClasses.gsod.GSOD;

/**
 * Preprocessing job to equi-join weather stations data with weather observations data, producing a
 * dataset of weather observations that includes the latitude and longitude of the observing weather
 * station.  Replicated join algorithm is used due to the small size of the weather station data.
 */
public class JoinWeatherWithStations extends Configured implements Tool {
  private static final Logger logger = LogManager.getLogger(JoinWeatherWithStations.class);
  private static final String FILE_LABEL = "fileLabel";

  public static class RepJoinMapper extends Mapper<Object, Text, NullWritable, GSOD> {
    // Using HashMap instead of MultiMap because each key identifies one station only
    private final Map<String, LatLon> weatherStationsMap = new HashMap<>();
    private final Set<String> missingStations = new HashSet<>();
    private final NullWritable nullKey = NullWritable.get();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      BufferedReader reader = getReaderFromFileCache(context);
      String record;
      while ((record = reader.readLine()) != null) {
        WeatherStation station = new WeatherStation(record);
        if (station.isValid()) {
          weatherStationsMap.put(station.getUSAF_WBAN(), station.getLocation());
        }
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
      GSOD gsod = GSOD.parseCSVFromNOAA(input.toString());

      LatLon location = weatherStationsMap.containsKey(gsod.getUSAF_WBAN()) ?
              weatherStationsMap.get(gsod.getUSAF_WBAN()) :
              weatherStationsMap.get(gsod.getUSAF() + "_" + WeatherStation.DEFAULT_WBAN);

      if (location == null) {
        missingStations.add(gsod.getUSAF_WBAN());
        return;
      }

      gsod.setLocation(location);
      context.write(nullKey, gsod);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      // report USAF/WBAN for weather observations that did not have a corresponding station
      for (String miss : missingStations) {
        logger.info("Did not find any station with USAF_WBAN = " + miss);
      }
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
    job.setMapperClass(RepJoinMapper.class);
    job.setNumReduceTasks(0);

    // Key and Value type for output
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(GSOD.class);

    // Path for input and output
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    // Set up distributed cache with a copy of weather station data
    job.addCacheFile(new URI(args[1] + "#" + FILE_LABEL));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(final String[] args) {
    if (args.length != 3) {
      throw new Error("Three arguments required: <inputFileToBeRead> <inputFileToBeBroadcast> <outputDir>");
    }

    try {
      ToolRunner.run(new JoinWeatherWithStations(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }

}