package project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import project.helperClasses.Flight;
import project.helperClasses.FlightOrGSOD;
import project.helperClasses.LatLon;
import project.helperClasses.RegionId;
import project.helperClasses.gsod.GSOD;

/**
 * Joins airplane flight records with weather observation records to produce a dataset of flights
 * that have weather data associated with either their origin or destination airport.  The datasets
 * are joined using 1-Bucket-Theta/1-Bucket-Random algorithm.  The theta function joins a flight
 * with a weather observation if they have the same date and the observation is within a specified
 * distance in miles to the destination or origin.
 * <p>
 * For 1-bucket implementation, user inputs number of weather records (S), number of flight records
 * (T), and the number of available worker machines (R).  S must be <= T.  If the distance radius is
 * high enough to match an observation to every airport, then the output size will be at least
 * double the flight input size.
 * <p>
 * The input datasets must be produced via preprocessing of datasets from NOAA and the U.S. DOT. See
 * jobs "JoinWeatherWithStations" and "JoinFlightsWithAirports".
 */
public class JoinFlightsWithWeather extends Configured implements Tool {
  private static final Logger logger = LogManager.getLogger(JoinFlightsWithWeather.class);

  /**
   * Maps all GSOD weather data to random rows within the A x B partition matrix.
   */
  public static class GSOD_Mapper extends Mapper<Object, Text, RegionId, FlightOrGSOD> {
    private final RegionId regionId = new RegionId();
    private int a;
    private int b;
    Random randGenerator = new Random();
    long totalEmittedGSODs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      a = context.getConfiguration().getInt("A", 0);
      b = context.getConfiguration().getInt("B", 0);
      totalEmittedGSODs = 0;
    }

    @Override
    public void map(final Object key, final Text input, final Context context)
            throws IOException, InterruptedException {
      GSOD gsod = GSOD.parseCSVWithLatLon(input.toString());

      // Emit this GSOD for all regions in the selected row
      int randRow = randGenerator.nextInt(a);
      int minKey = randRow * b;
      int maxKey = (randRow * b + b - 1);
      for (int i = minKey; i <= maxKey; i++) {
        regionId.set(i, true);
        context.write(regionId, new FlightOrGSOD(gsod));
        totalEmittedGSODs++;
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      Counter total = context.getCounter(
              "Partition Counters", "Emitted GSODs");
      total.increment(totalEmittedGSODs);
    }
  }

  /**
   * Maps all flight data to random columns within the A x B partition matrix.
   */
  public static class FlightMapper extends Mapper<Object, Text, RegionId, FlightOrGSOD> {
    private final RegionId regionId = new RegionId();
    private int a;
    private int b;
    Random randGenerator = new Random();
    long totalEmittedFlights;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      a = context.getConfiguration().getInt("A", 0);
      b = context.getConfiguration().getInt("B", 0);
      totalEmittedFlights = 0;
    }

    @Override
    public void map(final Object key, final Text input, final Context context)
            throws IOException, InterruptedException {
      Flight flight = Flight.parseCSVWithLatLon(input.toString());

      // Emit this Flight for all regions in the selected column
      int randCol = randGenerator.nextInt(b);
      int maxKey = (a - 1) * b + randCol;
      for (int i = randCol; i <= maxKey; i += b) {
        regionId.set(i, false);
        context.write(regionId, new FlightOrGSOD(flight));
        totalEmittedFlights++;
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      Counter total = context.getCounter(
              "Partition Counters", "Emitted Flights");
      total.increment(totalEmittedFlights);
    }
  }

  /**
   * Compares each received GSOD to each received Flight, producing a 'hit' when the dates match and
   * the weather observation is within a certain radius of the flight's origin or destination.
   * Secondary sort is used to reduce memory overhead.
   */
  public static class FlightGSODReducer extends Reducer<RegionId, FlightOrGSOD, NullWritable, Flight> {
    NullWritable nullKey = NullWritable.get();
    long totalHits = 0;
    double distanceRadius;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      distanceRadius = context.getConfiguration().getDouble("Distance Radius", 0.0);
    }

    @Override
    public void reduce(final RegionId key, final Iterable<FlightOrGSOD> values, final Context context)
            throws IOException, InterruptedException {
      List<GSOD> GSODs = new LinkedList<>();

      for (FlightOrGSOD o : values) {
        if (!o.isFlight()) {
          // GSOD list will be filled first due to secondary sort.
          GSODs.add(o.getGSOD());
        } else {
          // Compare the flight's origin and destination with each weather observation.
          // ASSUMPTION: the origin and destination for a flight will not match the same observation.
          Flight f = o.getFlight();
          for (GSOD g : GSODs) {
            if (dateLocationMatchOrigin(f, g)) {
              f.setOriginGSOD(g);
              context.write(nullKey, f);
              f.setOriginGSOD(null);
              totalHits++;
            } else if (dateLocationMatchDest(f, g)) {
              f.setDestGSOD(g);
              context.write(nullKey, f);
              f.setDestGSOD(null);
              totalHits++;
            }
          }
        }
      }
    }

    private boolean dateLocationMatchOrigin(Flight f, GSOD g) {
      if (!f.getDate().equals(g.getDate())) {
        return false;
      }
      LatLon flightLoc = f.getOriginLocation();
      LatLon destLoc = g.getLocation();
      return flightLoc.distanceInMiles(destLoc) <= distanceRadius;
    }

    private boolean dateLocationMatchDest(Flight f, GSOD g) {
      if (!f.getDate().equals(g.getDate())) {
        return false;
      }
      LatLon flightLoc = f.getDestLocation();
      LatLon destLoc = g.getLocation();
      return flightLoc.distanceInMiles(destLoc) <= distanceRadius;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      Counter totalH = context.getCounter(
              "Partition Counters", "Total Hits");
      totalH.increment(totalHits);
    }
  }

  /**
   * Partitioner to facilitate secondary sort. For composite key (regionID, isGSOD), only regionID
   * is considered for partitioning.
   */
  public static class SecondarySortPartitioner extends Partitioner<RegionId, FlightOrGSOD> {
    @Override
    public int getPartition(RegionId regionId, FlightOrGSOD flightOrGSOD, int i) {
      return regionId.getRegion();
    }
  }

  /**
   * GroupComparator to facilitate secondary sort. For composite key (regionID, isGSOD), only
   * regionID is considered for grouping.
   */
  public static class GroupComparator extends WritableComparator {
    protected GroupComparator() {
      super(RegionId.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      RegionId r1 = (RegionId) w1;
      RegionId r2 = (RegionId) w2;
      return Integer.compare(r1.getRegion(), r2.getRegion());
    }
  }


  @Override
  public int run(final String[] args) throws Exception {

    // Configuration
    final Configuration conf = getConf();
    final Job job = Job.getInstance(conf, "Flight-GSOD Join");
    job.setJarByClass(JoinFlightsWithWeather.class);
    final Configuration jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", ",");

    // Classes for mappers, reducer, partitioner, and grouping comparator
    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, GSOD_Mapper.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FlightMapper.class);
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.setReducerClass(FlightGSODReducer.class);
    job.setPartitionerClass(SecondarySortPartitioner.class);
    job.setGroupingComparatorClass(GroupComparator.class);

    // Key and Value type for output
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Flight.class);
    job.setMapOutputKeyClass(RegionId.class);
    job.setMapOutputValueClass(FlightOrGSOD.class);

    broadcastAB(args, job);
    job.getConfiguration().setDouble("Distance Radius", Double.parseDouble(args[6]));
    return job.waitForCompletion(true) ? 0 : 1;
  }

  // Determine A and B for 1-Bucket-Random then broadcast to all mappers
  private void broadcastAB(String[] args, Job job) throws IllegalArgumentException {
    final int s = Integer.parseInt(args[3]); // should correspond to size of smaller weather dataset
    final int t = Integer.parseInt(args[4]); // should correspond to size of larger flight dataset
    final int r = Integer.parseInt(args[5]); // should be number of reducer machines
    if (!(s <= t)) {
      throw new IllegalArgumentException("S must be less than or equal to T");
    }
    final double c = (double) s / (double) t;

    int a;
    int b;
    if (c < (double) 1 / (double) r) {
      a = 1;
      b = r;
    } else {
      a = (int) java.lang.Math.sqrt(c * r);
      b = (int) java.lang.Math.sqrt(r * (double) (1 / c));
    }
    job.getConfiguration().setInt("A", a);
    job.getConfiguration().setInt("B", b);
    job.setNumReduceTasks(a * b);
    logger.info("For theta-join: A is " + a + "; B is " + b);
  }

  public static void main(final String[] args) {
    if (args.length != 7) {
      throw new Error("Seven arguments required: <gsod_file> <flight_file> <outputDir> <S> <T> <R> <radiusMiles>");
    }

    try {
      ToolRunner.run(new JoinFlightsWithWeather(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }
}
