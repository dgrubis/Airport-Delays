package project.approachOne;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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
import project.helperClasses.gsod.GSOD;
import project.helperClasses.gsod.GSOD_Text;

public class JoinFlightsWithWeather extends Configured implements Tool {
  private static final Logger logger = LogManager.getLogger(JoinFlightsWithAirports.class);

  public static class GSOD_Mapper extends Mapper<Object, Text, IntWritable, FlightOrGSOD> {
    private final IntWritable regionId = new IntWritable();
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
      GSOD gsod = GSOD_Text.parseCSVWithLatLon(input.toString());

      // Emit this GSOD for all regions in the selected row
      int randRow = randGenerator.nextInt(a);
      int minKey = randRow * b;
      int maxKey = (randRow * b + b - 1);
      for (int i = minKey; i <= maxKey; i++) {
        regionId.set(i);
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

  public static class FlightMapper extends Mapper<Object, Text, IntWritable, FlightOrGSOD> {
    private final IntWritable regionId = new IntWritable();
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
        regionId.set(i);
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

  public static class FlightGSODReducer extends Reducer<IntWritable, FlightOrGSOD, NullWritable, Flight> {
    NullWritable nullKey = NullWritable.get();
    long totalHits = 0;
    double distanceRadius;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      distanceRadius = context.getConfiguration().getDouble("Distance Radius", 0.0);
    }

    @Override
    public void reduce(final IntWritable key, final Iterable<FlightOrGSOD> values, final Context context)
            throws IOException, InterruptedException {
      //TODO: secondary sort might make this more memory-efficient
      List<Flight> flights = new LinkedList<>();
      List<GSOD> GSODs = new LinkedList<>();

      // Populate the lists
      for (FlightOrGSOD o : values) {
        if (o.isFlight()) {
          flights.add(o.getFlight());
        } else {
          GSODs.add(o.getGSOD());
        }
      }

      // Compare each flight's origin and destination with each weather observation.
      // ASSUMPTION: the origin and destination for a flight will not match the same observation.
      for (Flight f : flights) {
        for (GSOD g : GSODs) {
          if (dateLocationMatchOrigin(f, g)) {
            f.setOriginGSOD(g);
            context.write(nullKey, f);
            totalHits++;
          } else if (dateLocationMatchDest(f, g)) {
            f.setDestGSOD(g);
            context.write(nullKey, f);
            totalHits++;
          } else {
            logger.info("No observation hits for flight: " + f.toString());
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
      Counter total = context.getCounter(
              "Partition Counters", "Total Hits");
      total.increment(totalHits);
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

    // Classes for mapper, combiner and reducer
    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, GSOD_Mapper.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FlightMapper.class);
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.setReducerClass(FlightGSODReducer.class);

    // Key and Value type for output
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Flight.class);
    job.setMapOutputKeyClass(IntWritable.class);
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
    logger.info("For theta-join: A is " + a + "; B is " + b);
  }

  public static void main(final String[] args) {
    if (args.length != 7) {
      throw new Error("Seven arguments required: <gsod_file> <flight_file> <outputDir> <S> <T> <R> <radius>");
    }

    try {
      ToolRunner.run(new JoinFlightsWithWeather(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }
}
