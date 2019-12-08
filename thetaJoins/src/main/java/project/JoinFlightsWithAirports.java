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
import java.util.Random;
import java.util.Set;

import project.helperClasses.Airport;
import project.helperClasses.Flight;
import project.helperClasses.LatLon;

/**
 * Preprocessing job to equi-join airport data with airplane flight record data, producing a dataset
 * of flights that includes the latitude and longitude of the origin/destination airports.
 * Replicated join algorithm is used due to the small size of the airport data.
 */
public class JoinFlightsWithAirports extends Configured implements Tool {

  private static final Logger logger = LogManager.getLogger(JoinFlightsWithAirports.class);
  private static final String FILE_LABEL = "fileLabel";

  public static class RepJoinMapper extends Mapper<Object, Text, NullWritable, Flight> {
    // Using HashMap instead of MultiMap because each key identifies one airport only
    private final Map<String, LatLon> airportsMap = new HashMap<>();
    private final Set<String> missingAirports = new HashSet<>();
    private final NullWritable nullKey = NullWritable.get();
    Random randGenerator = new Random(); // Used for random sampling of flight data
    private double sampleRate;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      BufferedReader reader = getReaderFromFileCache(context);
      String record;
      Airport airport;
      while ((record = reader.readLine()) != null) {
        try {
          airport = Airport.parseCSVFromDOT(record);
        } catch (IllegalArgumentException e) {
          continue;
        }
        airportsMap.put(airport.getIATA_Code(), airport.getLocation());
      }
      reader.close();

      // Get sample rate
      sampleRate = context.getConfiguration().getDouble("P", 0.0);
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
      // Randomly sample flight data
      double rand = randGenerator.nextDouble();
      if (rand >= sampleRate) {
        return;
      }

      Flight flight = Flight.parseCSVFromDOT(input.toString());
      LatLon origin = airportsMap.get(flight.getOriginIATA());
      if (origin == null) {
        missingAirports.add(flight.getOriginIATA());
        return;
      }
      LatLon dest = airportsMap.get(flight.getDestIATA());
      if (dest == null) {
        missingAirports.add(flight.getDestIATA());
        return;
      }

      flight.setOriginLocation(origin);
      flight.setDestLocation(dest);
      context.write(nullKey, flight);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      // report IATA code for flight origin/dest that did not have a corresponding airport
      for (String miss : missingAirports) {
        logger.info("Did not find any airport with IATA code = " + miss);
      }
    }
  }

  @Override
  public int run(final String[] args) throws Exception {

    // Configuration
    final Configuration conf = getConf();
    final Job job = Job.getInstance(conf, "Flight Join");
    job.setJarByClass(JoinWeatherWithStations.class);
    final Configuration jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", ",");

    // Classes for mapper, combiner and reducer
    job.setMapperClass(RepJoinMapper.class);
    job.setNumReduceTasks(0);

    // Key and Value type for output
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Flight.class);

    // Path for input and output
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    // Set up distributed cache with a copy of weather station data
    job.addCacheFile(new URI(args[1] + "#" + FILE_LABEL));

    // Broadcast user-specified sampling rate p
    double sampleRate = Double.parseDouble(args[3]);
    job.getConfiguration().setDouble("P", sampleRate);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(final String[] args) {
    if (args.length != 4) {
      throw new Error("Four arguments required: <inputFileToBeRead> <inputFileToBeBroadcast> <outputDir> <sampleRate>");
    }

    try {
      ToolRunner.run(new JoinFlightsWithAirports(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }
}
