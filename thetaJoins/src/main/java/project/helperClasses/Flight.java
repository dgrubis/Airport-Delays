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
  private LatLon originLocation;
  private LatLon destLocation;
  private Text data;

  public void setOriginLocation(LatLon originLocation) {
    this.originLocation = originLocation;
  }

  public void setDestLocation(LatLon destLocation) {
    this.destLocation = destLocation;
  }

  public String getOriginIATA() {
    return originIATA;
  }

  public String getDestIATA() {
    return destIATA;
  }

  /**
   * Parse a flight record from a Department of Transportation (DOT) record string. LatLon
   * information will be null.
   *
   * @param record an input record as described in DOT flight documentation
   * @return a parsed Flight
   */
  public static Flight parseCSVFromDOT(String record) {
    Flight flight = new Flight();
    flight.originLocation = null;
    flight.destLocation = null;

    String[] tokens = record.split(",");
    int year = Integer.parseInt(tokens[0]);
    int month = Integer.parseInt(tokens[1]);
    int day = Integer.parseInt(tokens[2]);
    flight.date = LocalDate.of(year, month, day);
    flight.originIATA = tokens[7];
    flight.destIATA = tokens[8];

    String data = "";
    data += tokens[4]; // Airline
    data += "," + tokens[17]; // Distance in miles
    data += tokens.length > 25 && tokens[25].equals("B") ? ",1" : ",0"; // Flag for weather cancellation
    data += tokens.length == 31 ? "," + tokens[30] + "," : ",0,"; // Weather delay in minutes
    flight.data = new Text(data);

    return flight;
  }


  @Override
  public void write(DataOutput out) throws IOException {
    if (originLocation == null || destLocation == null) {
      throw new IllegalStateException("Cannot emit a Flight without location data.");
    }
    out.writeInt(date.getYear());
    out.writeInt(date.getMonthValue());
    out.writeInt(date.getDayOfMonth());
    out.writeChars(originIATA + "\n");
    out.writeChars(destIATA + "\n");
    out.writeDouble(originLocation.getLatitude());
    out.writeDouble(originLocation.getLongitude());
    out.writeDouble(destLocation.getLatitude());
    out.writeDouble(destLocation.getLongitude());
    data.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {

    int year = in.readInt();
    int month = in.readInt();
    int day = in.readInt();
    date = LocalDate.of(year, month, day);
    originIATA = in.readLine();
    destIATA = in.readLine();

    double latitude = in.readDouble();
    double longitude = in.readDouble();
    originLocation = new LatLon(latitude, longitude);
    latitude = in.readDouble();
    longitude = in.readDouble();
    destLocation = new LatLon(latitude, longitude);

    data = new Text();
    data.readFields(in);
  }

  @Override
  public String toString() {
    if (originLocation == null || destLocation == null) {
      // DATE,ORIGIN,DEST,AIRLINE,DISTANCE,WEATHER_CANCELLATION,WEATHER_DELAY
      return date.toString() + "," + originIATA + "," + destIATA + "," + data;
    }

    // DATE,ORIGIN,ORIGIN_LAT,ORIGIN_LONG,DEST,DEST_LAT,DEST_LON,AIRLINE,DISTANCE,WEATHER_CANCELLATION,WEATHER_DELAY
    return date.toString() + "," + originIATA + "," + originLocation + "," + destIATA + "," + destLocation + "," + data;
  }
}