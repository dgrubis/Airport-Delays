package project.helperClasses.gsod;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDate;
import java.util.StringTokenizer;

import project.helperClasses.LatLon;

/**
 * Global surface summary of the day produced by the National Climatic Data Center (NCDC) and
 * available through the National Oceanic and Atmospheric Administration (NOAA).  An instance of
 * this class represents the GSOD data from a particular weather station on a particular day.  An
 * instance can store USAF/WBAN data, but will not emit it from mappers or reducers.
 * <p>
 * This version of a GSOD is more efficient than value-based GSOD storage, but cannot be used to
 * average GSOD values.
 */
//TODO: the version of GSOD_Text with location data can be a separate class extending GSOD_Text
public class GSOD implements Writable {
  private String USAF; // Air Force Station ID
  private String WBAN; // Weather Bureau Air Force Navy number
  private LocalDate date;
  private LatLon location;
  private String data;

  public String getUSAF_WBAN() {
    return USAF + "_" + WBAN;
  }

  public String getUSAF() {
    return USAF;
  }

  public LocalDate getDate() {
    return date;
  }

  public LatLon getLocation() {
    return location;
  }

  public void setLocation(LatLon location) {
    this.location = location;
  }

  /**
   * Parse a GSOD from a NOAA weather observation record string. Location data will be null.  MAX,
   * MIN, PRCP, and SNDP will be modified to strip extraneous flags and/or to reflect the data's
   * actual value.  See NOAA documentation.  Data in the form 99.9/999.9/9999.9 indicates a missing
   * data point.
   *
   * @param record an input record as described in NOAA weather documentation
   * @return a parsed GSOD
   */
  public static GSOD parseCSVFromNOAA(String record) {
    String[] splitRecord = record.split(",\\s*");
    GSOD parsedGSOD = new GSOD();

    // TODO: many of these values are represented as NULL via a 9999.9/999.9/99.9 entry. See documentation.
    //  Do we need to change this representation?

    parsedGSOD.USAF = splitRecord[0];
    parsedGSOD.WBAN = splitRecord[1];
    parsedGSOD.date = parseDate(splitRecord[2]);
    parsedGSOD.location = null; // Must be added separately or parsed using a different method

    String dataString = splitRecord[3] +
            "," +
            splitRecord[5] +
            "," +
            splitRecord[7] +
            "," +
            splitRecord[9] +
            "," +
            splitRecord[11] +
            "," +
            splitRecord[13] +
            "," +
            splitRecord[15] +
            "," +
            splitRecord[16] +
            "," +
            parseMaxMinTemp(splitRecord[17]) +
            "," +
            parseMaxMinTemp(splitRecord[18]) +
            "," +
            parsePrecipitation(splitRecord[19]) +
            "," +
            parseSnowDepth(splitRecord[20]) +
            "," +
            splitRecord[21] +
            ",";
    parsedGSOD.data = dataString;

    return parsedGSOD;
  }

  private static LocalDate parseDate(String date) {
    int year = Integer.parseInt(date.substring(0, 4));
    int month = Integer.parseInt(date.substring(4, 6));
    int day = Integer.parseInt(date.substring(6, 8));
    return LocalDate.of(year, month, day);
  }

  private static String parseMaxMinTemp(String temp) {
    return temp.endsWith("*") || temp.endsWith(" ") ? temp.substring(0, temp.length() - 1) : temp;
  }

  private static String parsePrecipitation(String p) {
    // Per documentation, 99.9 suggests no precipitation (different from other attributes where
    // 999... suggests missing data)
    if (p.equals("99.99")) {
      return "0.00";
    }
    if (p.endsWith("I") || p.endsWith("H")) {
      // Per documentation, I or H flag indicates missing or incomplete data.
      return "99.99";
    }
    // Remove extraneous flags
    int lastIndex = p.length() - 1;
    if (Character.isLetter(p.charAt(lastIndex))) {
      return p.substring(0, lastIndex);
    }
    return p;
  }

  private static String parseSnowDepth(String snow) {
    // Per documentation, 999.9 suggests no snow (different from other attributes where
    // 999... suggests missing data)
    if (snow.equals("999.9")) {
      return "0.0";
    }
    return snow;
  }

  /**
   * Parse a GSOD from a String that contains location data (obtained from toString() of another
   * GSOD). USAF/WBAN data will be null. Data in the form 99.9/999.9/9999.9 indicates a missing data
   * point.  Should not be used to parse raw GSOD data from NOAA (see parseCSVFromNOAA).
   *
   * @param record an input record produced by another GSOD
   * @return a parsed GSOD
   */
  public static GSOD parseCSVWithLatLon(String record) {
    GSOD parsedGSOD = new GSOD();
    StringTokenizer tokens = new StringTokenizer(record, ",\\s*");
    parsedGSOD.USAF = null; // Must be obtained via a different parsing function
    parsedGSOD.WBAN = null;
    parsedGSOD.date = LocalDate.parse(tokens.nextToken());
    String lat = tokens.nextToken();
    String lon = tokens.nextToken();
    parsedGSOD.location = new LatLon(lat, lon);
    parsedGSOD.data = tokens.nextToken("\n").substring(1).replace(" ", "");

    return parsedGSOD;
  }


  @Override
  public void write(DataOutput out) throws IOException {
    if (location == null) {
      throw new IllegalStateException("Cannot emit a GSOD without location data.");
    }
    out.writeInt(date.getYear());
    out.writeInt(date.getMonthValue());
    out.writeInt(date.getDayOfMonth());
    out.writeDouble(location.getLatitude());
    out.writeDouble(location.getLongitude());
    out.writeBytes(data + "\n");
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int year = in.readInt();
    int month = in.readInt();
    int day = in.readInt();
    date = LocalDate.of(year, month, day);

    double latitude = in.readDouble();
    double longitude = in.readDouble();
    location = new LatLon(latitude, longitude);
    data = in.readLine();
  }

  @Override
  public String toString() {
    if (location == null) {
      //Format: DATE,TEMP,DEWP,SLP,STP,VISIB,WDSP,MXSPD,GUST,MAX,MIN,PRCP,SNDP,FRSHTT,
      return date + "," + data;
    }

    //Format: DATE,LATITUDE,LONGITUDE,TEMP,DEWP,SLP,STP,VISIB,WDSP,MXSPD,GUST,MAX,MIN,PRCP,SNDP,FRSHTT,
    return date + "," + location + "," + data;
  }
}

