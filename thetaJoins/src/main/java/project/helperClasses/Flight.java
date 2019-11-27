package project.helperClasses;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDate;

/**
 * U.S. domestic large flight data published by the Department of Transportation (DOT).  An instance
 * of this class represents a single flight.
 */
public class Flight implements Writable {

  private LocalDate date;
  private String originIATA;
  private String destIATA;
  private Text data;

  /**
   * Parse a flight record from a Department of Transportation (DOT) record string.
   *
   * @param record an input record as described in DOT flight documentation
   * @return a parsed Flight
   */
  public static Flight parseCSVFromDOT(String record) {
    Flight flight = new Flight();
    String[] tokens = record.split(",");
    String data = "";

    int year = Integer.parseInt(tokens[0]);
    int month = Integer.parseInt(tokens[1]);
    int day = Integer.parseInt(tokens[2]);
    flight.date = LocalDate.of(year, month, day);
    data += tokens[4]; // Include Airline
    flight.originIATA = tokens[7];
    flight.destIATA = tokens[8];
    data += "," + tokens[17]; // Include distance in miles
    data += tokens.length > 25 && tokens[25].equals("B") ? ",1" : ",0"; // Flag for weather cancellation
    data += tokens.length == 31 ? "," + tokens[30] + "," : ",0,"; // Weather delay in minutes


    flight.data = new Text(data);
    return flight;
  }


  @Override
  public void write(DataOutput dataOutput) throws IOException {

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {

  }

  @Override
  public String toString() {
    // DATE,ORIGIN,DEST,AIRLINE,DISTANCE,WEATHER_CANCELLATION,WEATHER_DELAY
    return date.toString() + "," + originIATA + "," + destIATA + "," + data;
  }
}
