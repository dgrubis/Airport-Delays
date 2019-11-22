package project.helperClasses.gsod;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDate;

import project.helperClasses.LatLong;

/**
 * Global surface summary of the day produced by the National Climatic Data Center (NCDC) and
 * available through the National Oceanic and Atmospheric Administration (NOAA).  An instance of
 * this class represents the GSOD data from a particular weather station on a particular day.  An
 * instance can store USAF/WBAN data, but will not emit it from mappers or reducers.
 * <p>
 * This version of a GSOD is more efficient than value-based GSOD storage, but cannot be used to
 * average GSOD values;
 */
public class GSOD_Text implements Writable, GSOD {
  private String USAF_WBAN; // Air Force Station ID and Weather Bureau Air Force Navy number
  private LocalDate date;
  private LatLong location; // Must be provided from another dataset
  private Text data;

  @Override
  public String getUSAF_WBAN() {
    return USAF_WBAN;
  }

  @Override
  public void setLocation(LatLong location) {
    this.location = location;
  }

  /**
   * Parse a GSOD from a NOAA weather observation record string. Location data will be null.
   *
   * @param record an input record as described in NOAA weather documentation
   * @returna parsed GSOD
   */
  public static GSOD_Text parseCSVFromNOAA(String record) {
    String[] splitRecord = record.split(",\\s*");
    GSOD_Text parsedGSOD = new GSOD_Text();

    // TODO: many of these values are represented as NULL via a 9999.9/999.9/99.9 entry. See documentation.
    //  Do we need to change this representation?

    parsedGSOD.USAF_WBAN = splitRecord[0] + splitRecord[1];
    parsedGSOD.date = parseDate(splitRecord[2]);
    parsedGSOD.location = null;

    StringBuilder dataString = new StringBuilder();
    dataString.append(splitRecord[3]);
    dataString.append(",");
    dataString.append(splitRecord[5]);
    dataString.append(",");
    dataString.append(splitRecord[7]);
    dataString.append(",");
    dataString.append(splitRecord[9]);
    dataString.append(",");
    dataString.append(splitRecord[11]);
    dataString.append(",");
    dataString.append(splitRecord[13]);
    dataString.append(",");
    dataString.append(splitRecord[15]);
    dataString.append(",");
    dataString.append(splitRecord[16]);
    dataString.append(",");
    dataString.append(parseMaxMinTemp(splitRecord[17]));
    dataString.append(",");
    dataString.append(parseMaxMinTemp(splitRecord[18]));
    dataString.append(",");
    dataString.append(parsePrecipitation(splitRecord[19]));
    dataString.append(",");
    dataString.append(parseSnowDepth(splitRecord[20]));
    dataString.append(",");
    dataString.append(splitRecord[21]);
    dataString.append(",");
    parsedGSOD.data = new Text(dataString.toString());

    return parsedGSOD;
  }

  private static LocalDate parseDate(String date) {
    int year = Integer.parseInt(date.substring(0, 4));
    int month = Integer.parseInt(date.substring(4, 6));
    int day = Integer.parseInt(date.substring(6, 8));
    return LocalDate.of(year, month, day);
  }

  private static String parseMaxMinTemp(String temp) {
    return temp.endsWith("*") ? temp.substring(0, temp.length() - 1) : temp;
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
    data.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int year = in.readInt();
    int month = in.readInt();
    int day = in.readInt();
    this.date = LocalDate.of(year, month, day);

    double latitude = in.readDouble();
    double longitude = in.readDouble();
    this.location = new LatLong(latitude, longitude);

    this.data = new Text();
    data.readFields(in);
  }
}

