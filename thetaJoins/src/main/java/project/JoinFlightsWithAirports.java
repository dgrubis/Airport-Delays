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

import project.helperClasses.Airport;
import project.helperClasses.Flight;
import project.helperClasses.LatLon;

// TODO: a portion of flights are identifying airports by a value other than IATA. See log outputs.
//  A handful of airports also do not provide lat/lon. Close to 500,000 flights do not have airports.
public class JoinFlightsWithAirports extends Configured implements Tool {

  private static final Logger logger = LogManager.getLogger(JoinFlightsWithAirports.class);
  private static final String FILE_LABEL = "fileLabel";

  public static class repJoinMapper extends Mapper<Object, Text, NullWritable, Flight> {
    // Using HashMap instead of MultiMap because each key identifies one airport only
    private Map<String, LatLon> airportsMap = new HashMap<>();
    private Set<String> missingAirports = new HashSet<>();
    private NullWritable nullKey = NullWritable.get();

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
    job.setMapperClass(repJoinMapper.class);
    job.setNumReduceTasks(0);

    // Key and Value type for output
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Flight.class);

    // Path for input and output
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    // Set up distributed cache with a copy of weather station data
    job.addCacheFile(new URI(args[1] + "#" + FILE_LABEL));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(final String[] args) {
    if (args.length != 3) {
      throw new Error("Three arguments required:\n<inputFileToBeRead> <inputFileToBeBroadcast> <outputDir>");
    }

    try {
      ToolRunner.run(new JoinFlightsWithAirports(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }
}
