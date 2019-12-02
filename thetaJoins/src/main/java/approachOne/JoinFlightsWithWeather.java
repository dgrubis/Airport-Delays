package approachOne;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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

import project.helperClasses.Flight;
import project.helperClasses.FlightOrGSOD;
import project.helperClasses.gsod.GSOD;
import project.helperClasses.gsod.GSOD_Text;

public class JoinFlightsWithWeather extends Configured implements Tool {

  private static final Logger logger = LogManager.getLogger(JoinFlightsWithAirports.class);

  public static class GSOD_Mapper extends Mapper<Object, Text, IntWritable, FlightOrGSOD> {
    private final IntWritable regionId = new IntWritable();
    private int a;
    private int b;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      a = context.getConfiguration().getInt("A", 0);
      b = context.getConfiguration().getInt("B", 0);
    }

    @Override
    public void map(final Object key, final Text input, final Context context)
            throws IOException, InterruptedException {
      GSOD gsod = GSOD_Text.parseCSVWithLatLon(input.toString());
      // TODO: replace with 1-Bucket row assignment
      regionId.set(0);
      context.write(regionId, new FlightOrGSOD(gsod));
    }
  }

  public static class FlightMapper extends Mapper<Object, Text, IntWritable, FlightOrGSOD> {
    private final IntWritable regionId = new IntWritable();
    private int a;
    private int b;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      a = context.getConfiguration().getInt("A", 0);
      b = context.getConfiguration().getInt("B", 0);
    }

    @Override
    public void map(final Object key, final Text input, final Context context)
            throws IOException, InterruptedException {
      Flight flight = Flight.parseCSVWithLatLon(input.toString());
      // TODO: replace with 1-Bucket column assignment
      regionId.set(0);
      context.write(regionId, new FlightOrGSOD(flight));
    }
  }

  public static class FlightGSODReducer extends Reducer<IntWritable, FlightOrGSOD, NullWritable, Flight> {
    NullWritable nullKey = NullWritable.get();

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

      for (Flight f : flights) {
        for (GSOD g : GSODs) {
          if (dateAndLocationMatch(f, g)) {
            // TODO: emit some stuff
          }
        }
      }
    }

    private boolean dateAndLocationMatch(Flight f, GSOD g) {
      // TODO: write this function
      return true;
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
  }

  public static void main(final String[] args) {
    if (args.length != 6) {
      throw new Error("Three arguments required: <gsod_file> <flight_file> <outputDir> <S> <T> <R>");
    }

    try {
      ToolRunner.run(new JoinFlightsWithWeather(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }
}
