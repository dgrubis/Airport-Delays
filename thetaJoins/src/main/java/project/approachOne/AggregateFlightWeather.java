package project.approachOne;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import project.helperClasses.LatLon;

/**
 * Combines the Flight + Origin Weather and Flight + Destination Weather data to
 * provide Flight + Origin Weather + Destination Weather Data.
 */
public class AggregateFlightWeather extends Configured implements Tool {
  private static final Logger logger = LogManager.getLogger(AggregateFlightWeather.class);
  private static final int FOrigin_Lat = 2;
  private static final int FOrigin_Lon = 3;
  private static final int FDest_Lat = 5;
  private static final int FDest_Lon = 6;
  private static final int WOrigin_Lat = 15;
  private static final int WOrigin_Lon = 16;
  private static final int WDest_Lat = 17;
  private static final int WDest_Lon = 18;
  private static final int null_field = 14;
  private static final int dest_data_start = 16;
  // Key fields:
  private static final int key1 = 0;
  private static final int key2 = 1;
  private static final int key3 = 4;
  private static final int key4 = 8;
  private static final int key5 = 9;

  public static class FlightKeyMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(final Object key, final Text input, final Context context)
        throws IOException, InterruptedException {
      String[] data = input.toString().split(",");
      // Set flight ID as key and whole row as the value
      context.write(new Text(data[key1] + data[key2] + data[key3] + data[key4] + data[key5]), input);
    }
  }

  public static class FlightAggregationReducer extends Reducer<Text, Text, NullWritable, Text> {
    NullWritable nullKey = NullWritable.get();
    int miss_counter = 0;

    @Override
    protected void reduce(Text flightKey, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      Text min_origin = null; // The weather station data found closest to the flight origin
      Text min_dest = null; // The weather station data found closest to the flight destination
      int min_origin_dist = Integer.MAX_VALUE, min_dest_dist = Integer.MAX_VALUE;

      for (Text text : values) {
        String[] data = text.toString().split(",");

        if (data[null_field].equals("null")) {
          // The row contains flight + destination weather
          LatLon FD = new LatLon(data[FDest_Lat], data[FDest_Lon]);
          LatLon WD = new LatLon(data[WDest_Lat], data[WDest_Lon]);
          if ((int) FD.distanceInMiles(WD) < min_dest_dist) {
            min_dest = new Text(text);
            min_dest_dist = (int) FD.distanceInMiles(WD);
          }
        }

        else {
          // The row contains flight + origin weather
          LatLon FO = new LatLon(data[FOrigin_Lat], data[FOrigin_Lon]);
          LatLon WO = new LatLon(data[WOrigin_Lat], data[WOrigin_Lon]);
          if ((int) FO.distanceInMiles(WO) < min_origin_dist) {
            min_origin = new Text(text);
            min_origin_dist = (int) FO.distanceInMiles(WO);
          }
        }
      }

      if (min_origin != null && min_dest != null) {
        String[] OriginData = min_origin.toString().split(",");
        String[] DestData = min_dest.toString().split(",");

        String[] FinalData = new String[150];

        FinalData[0] = OriginData[0];
        int i = 1;
        // Copy the origin weather station data
        for (int k = 1; k < OriginData.length; k++, i += 2) {
          FinalData[i] = ",";
          FinalData[i + 1] = OriginData[k];
        }

        // Copy the destination weather station data
        for (int j = dest_data_start; j < DestData.length; j++, i += 2) {
          FinalData[i] = ",";
          FinalData[i + 1] = DestData[j];
        }

        // Obtain string from string array
        String str = "";
        for (i = 0; i < FinalData.length; i++) {
          if (i > 50 && FinalData[i] == null)
            continue;
          str += FinalData[i];
        }

        context.write(nullKey, new Text(str));
      }
      else{
        miss_counter++;
      }
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
      Counter total = context.getCounter(
              "Missed Flight Counter", "No Origin or Weather Station");
      total.increment(miss_counter);
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
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

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