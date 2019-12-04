package project.helperClasses;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.StringTokenizer;

import project.helperClasses.gsod.GSOD;
import project.helperClasses.gsod.GSOD_Text;

/**
 * U.S. domestic large flight data published by the Department of Transportation (DOT).  An instance
 * of this class represents a single flight.
 */
// TODO: the variation on this class (with or without location or climate data) can be separated into classes that extend Flight
//TODO: origin and destination can be their own classes
public class Flight implements Writable {

  private LocalDate date;
  private String originIATA;
  private String destIATA;
  private LatLon originLocation; // Use when joining with location data
  private LatLon destLocation; // Use when joining with location data
  private String data;
  private GSOD originGSOD; // Use when joining with weather data
  private GSOD destGSOD; // Use when joining with weather data

  public void setOriginLocation(LatLon originLocation) {
    this.originLocation = originLocation;
  }

  public void setDestLocation(LatLon destLocation) {
    this.destLocation = destLocation;
  }

  public void setOriginGSOD(GSOD originGSOD) {
    this.originGSOD = originGSOD;
  }

  public void setDestGSOD(GSOD destGSOD) {
    this.destGSOD = destGSOD;
  }

  public String getOriginIATA() {
    return originIATA;
  }

  public String getDestIATA() {
    return destIATA;
  }

  public LocalDate getDate() {
    return date;
  }

  public LatLon getOriginLocation() {
    return originLocation;
  }

  public LatLon getDestLocation() {
    return destLocation;
  }

  /**
   * Parse a flight record from a Department of Transportation (DOT) record string. LatLon and
   * weather information will be null.
   *
   * @param record an input record as described in DOT flight documentation
   * @return a parsed Flight
   */
  public static Flight parseCSVFromDOT(String record) {

    Flight flight = new Flight();
    flight.originLocation = null;
    flight.destLocation = null;
    flight.originGSOD = null;
    flight.destGSOD = null;

    String[] tokens = record.split(",\\s*");
    int year = Integer.parseInt(tokens[0]);
    int month = Integer.parseInt(tokens[1]);
    int day = Integer.parseInt(tokens[2]);
    flight.date = LocalDate.of(year, month, day);
    flight.originIATA = tokens[7];
    flight.destIATA = tokens[8];

    String data = "";
    data += tokens[4]; // Airline
    data += "," + tokens[5]; // Flight number
    data += "," + tokens[6]; // Tail number
    data += "," + tokens[17]; // Distance in miles
    data += tokens.length > 25 && tokens[25].equals("B") ? ",1" : ",0"; // Flag for weather cancellation
    data += tokens.length == 31 ? "," + tokens[30] + "," : ",0,"; // Weather delay in minutes
    flight.data = data;

    return flight;
  }

  /**
   * Parse a flight record from a record string produced by a Flight instance. Weather information
   * will be null.
   *
   * @param record an flight input string
   * @return a parsed Flight
   */
  public static Flight parseCSVWithLatLon(String record) {
    Flight flight = new Flight();
    flight.originGSOD = null;
    flight.destGSOD = null;
    StringTokenizer tokens = new StringTokenizer(record, ",\\s*");
    flight.date = LocalDate.parse(tokens.nextToken());
    flight.originIATA = tokens.nextToken();
    String lat = tokens.nextToken();
    String lon = tokens.nextToken();
    flight.originLocation = new LatLon(lat, lon);
    flight.destIATA = tokens.nextToken();
    lat = tokens.nextToken();
    lon = tokens.nextToken();
    flight.destLocation = new LatLon(lat, lon);
    flight.data = tokens.nextToken("\n").substring(1);

    return flight;
  }

  public static Flight parseCSVWithLatLonWeather(String record) {
    Flight flight = new Flight();

    String[] tokens = record.split(",\\s*");
    if (tokens.length < 30) {
      throw new IllegalArgumentException("Invalid input string. Data is missing");
    }

    // Parse flight data:
    flight.date = LocalDate.parse(tokens[0]);
    flight.originIATA = tokens[1];
    flight.originLocation = new LatLon(tokens[2], tokens[3]);
    flight.destIATA = tokens[4];
    flight.destLocation = new LatLon(tokens[5], tokens[6]);
    flight.data = tokens[7] + "," + tokens[8] + "," + tokens[9] + "," + tokens[10] + ","
            + tokens[11] + "," + tokens[12] + ",";

    // If origin weather data is found:
    if (!tokens[13].equals("null")) {
      String gsodOrig = Arrays.toString(Arrays.copyOfRange(tokens, 13, 29));
      flight.originGSOD = GSOD_Text.parseCSVWithLatLon(gsodOrig.substring(1, gsodOrig.length() - 1) + ",");

      // If origin and destination weather data is found:
      if (!tokens[30].equals("null")) {
        String gsodDest = Arrays.toString(Arrays.copyOfRange(tokens, 30, 46));
        flight.destGSOD = GSOD_Text.parseCSVWithLatLon(gsodDest.substring(1, gsodDest.length() - 1) + ",");
      } else {
        flight.destGSOD = null;
      }

      // If only destination weather data is found:
    } else {
      flight.originGSOD = null;
      String gsodString = Arrays.toString(Arrays.copyOfRange(tokens, 15, 31));
      flight.destGSOD = GSOD_Text.parseCSVWithLatLon(gsodString.substring(1, gsodString.length() - 1) + ",");
    }

    return flight;
  }

  @Override
  // Will not write weather information (GSOD)
  public void write(DataOutput out) throws IOException {
    if (originLocation == null || destLocation == null) {
      throw new IllegalStateException("Cannot emit a Flight without location data.");
    }
    if (originGSOD != null && destGSOD != null) {
      throw new IllegalArgumentException(
              "There is no need to emit a Flight with both origin and destination weather data.");
    }

    // Send flight data
    out.writeInt(date.getYear());
    out.writeInt(date.getMonthValue());
    out.writeInt(date.getDayOfMonth());
    out.writeBytes(originIATA + "\n");
    out.writeBytes(destIATA + "\n");
    out.writeDouble(originLocation.getLatitude());
    out.writeDouble(originLocation.getLongitude());
    out.writeDouble(destLocation.getLatitude());
    out.writeDouble(destLocation.getLongitude());

    // Send weather data as applicable
    boolean hasWeatherData = false;
    boolean hasOriginWeatherData = false;
    if (originGSOD != null) {
      hasWeatherData = true;
      hasOriginWeatherData = true;
      out.writeBoolean(hasWeatherData);
      out.writeBoolean(hasOriginWeatherData);
    } else if (destGSOD != null) {
      hasWeatherData = true;
      out.writeBoolean(hasWeatherData);
      out.writeBoolean(hasOriginWeatherData);
    } else {
      out.writeBoolean(hasWeatherData);
    }

    // Send text data
    out.writeBytes(data + "\n");
  }

  @Override
  // Will not read weather information (GSOD)
  public void readFields(DataInput in) throws IOException {

    // Read flight data
    int year = in.readInt();
    int month = in.readInt();
    int day = in.readInt();
    date = LocalDate.of(year, month, day);
    originIATA = in.readLine();
    destIATA = in.readLine();

    // Read location data
    double latitude = in.readDouble();
    double longitude = in.readDouble();
    originLocation = new LatLon(latitude, longitude);
    latitude = in.readDouble();
    longitude = in.readDouble();
    destLocation = new LatLon(latitude, longitude);

    // Read weather data as applicable
    originGSOD = null;
    destGSOD = null;
    if (in.readBoolean()) { // if weather data exists
      if (in.readBoolean()) { // and if it is origin weather data
        originGSOD = new GSOD_Text();
        originGSOD.readFields(in);
      }
      else { // or if it is destination weather data
        destGSOD = new GSOD_Text();
        destGSOD.readFields(in);
      }
    }

    // Read text data
    data = in.readLine();
  }

  @Override
  public String toString() {
    if (originLocation == null || destLocation == null) {
      // DATE,ORIGIN,DEST,AIRLINE,FLIGHT_NUMBER,TAIL_NUMBER,DISTANCE,WEATHER_CANCELLATION,WEATHER_DELAY,
      return date.toString() + "," + originIATA + "," + destIATA + "," + data;
    }

    if (originGSOD == null && destGSOD == null) {
      // DATE,ORIGIN,ORIGIN_LAT,ORIGIN_LONG,DEST,DEST_LAT,DEST_LON,AIRLINE,FLIGHT_NUMBER,TAIL_NUMBER,DISTANCE,WEATHER_CANCELLATION,WEATHER_DELAY,
      return date.toString() + "," + originIATA + "," + originLocation + ","
              + destIATA + "," + destLocation + "," + data;
    }

    String spaceBetweenWeather = ",";
    if (originGSOD == null) {
      spaceBetweenWeather = ",,";
    }

    // DATE,ORIGIN,ORIGIN_LAT,ORIGIN_LONG,DEST,DEST_LAT,DEST_LON,AIRLINE,FLIGHT_NUMBER,TAIL_NUMBER,DISTANCE,WEATHER_CANCELLATION,WEATHER_DELAY,ORIGIN_GSOD,,DEST_GSOD,
    return date.toString() + "," + originIATA + "," + originLocation + ","
            + destIATA + "," + destLocation + "," + data
            + originGSOD + spaceBetweenWeather + destGSOD;
  }
}
