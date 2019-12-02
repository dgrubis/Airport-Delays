package project.helperClasses.gsod;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDate;
import java.util.PrimitiveIterator;

import project.helperClasses.LatLon;

// TODO: this class is not finished or tested. Use if we need to average weather observation data

/**
 * Global surface summary of the day produced by the National Climatic Data Center (NCDC) and
 * available through the National Oceanic and Atmospheric Administration (NOAA).  An instance of
 * this class represents the GSOD data from a particular weather station on a particular day.  An
 * instance can store USAF/WBAN data, but will not emit it from mappers or reducers.
 * <p>
 * This version of a GSOD is less efficient than text-based GSOD storage, but can be used to average
 * GSODs values, if needed.
 */
public class GSOD_Values implements Writable, GSOD {
  // TODO: will we need double precision here? - initial data only goes to 2 decimal places.
  private String USAF; // Air Force Station ID
  private String WBAN; // Weather Bureau Air Force Navy number
  private LocalDate date;
  private float meanTempFahrenheit;
  private float dewPointFahrenheit;
  private float seaLevelPressureMillibars;
  private float stationPressureMillibars;
  private float visibilityMiles;
  private float windSpeedKnots;
  private float maxSustainedWindSpeedKnots;
  private float maxWindGustKnots;
  private float maxTempFahrenheit;  // TODO: Do we care about max and min temp, or just mean?
  private float minTempFahrenheit;
  private float precipitationInches; // Total rain and/or melted snow
  private float snowDepthInches;
  private boolean fog;
  private boolean rainOrDrizzle;
  private boolean snowOrIcePellets;
  private boolean hail;
  private boolean thunder;
  private boolean tornadoOrFunnelCloud;
  private LatLon location; // Must be provided from another dataset

  @Override
  public String getUSAF_WBAN() {
    return USAF + "_" + WBAN;
  }

  @Override
  public String getUSAF() {
    return USAF;
  }

  @Override
  public LocalDate getDate() {
    return date;
  }

  @Override
  public LatLon getLocation() {
    return location;
  }

  @Override
  public void setLocation(LatLon location) {
    this.location = location;
  }

  /**
   * Parse a GSOD from a NOAA weather observation record string. Location data will be null.
   *
   * @param record an input record as described in NOAA weather documentation
   * @returna parsed GSOD
   */
  public static GSOD_Values parseCSVFromNOAA(String record) {
    String[] splitRecord = record.split(",\\s*");
    GSOD_Values parsedGSOD = new GSOD_Values();

    // TODO: many of these values are represented as NULL via a 9999.9/999.9/99.9 entry. See documentation.
    //  Do we need to change this representation?

    parsedGSOD.USAF = splitRecord[0];
    parsedGSOD.WBAN = splitRecord[1];
    parsedGSOD.date = parseDate(splitRecord[2]);
    parsedGSOD.meanTempFahrenheit = Float.parseFloat(splitRecord[3]);
    parsedGSOD.dewPointFahrenheit = Float.parseFloat(splitRecord[5]);
    parsedGSOD.seaLevelPressureMillibars = Float.parseFloat(splitRecord[7]);
    parsedGSOD.stationPressureMillibars = Float.parseFloat(splitRecord[9]);
    parsedGSOD.visibilityMiles = Float.parseFloat(splitRecord[11]);
    parsedGSOD.windSpeedKnots = Float.parseFloat(splitRecord[13]);
    parsedGSOD.maxSustainedWindSpeedKnots = Float.parseFloat(splitRecord[15]);
    parsedGSOD.maxWindGustKnots = Float.parseFloat(splitRecord[16]);
    parsedGSOD.maxTempFahrenheit = parseMaxMinTemp(splitRecord[17]);
    parsedGSOD.minTempFahrenheit = parseMaxMinTemp(splitRecord[18]);
    parsedGSOD.precipitationInches = parsePrecipitation(splitRecord[19]);
    parsedGSOD.snowDepthInches = parseSnowDepth(splitRecord[20]);
    parseWeatherIndicators(parsedGSOD, splitRecord[21]); //TODO: consider keeping these as a single number until training
    parsedGSOD.location = null;

    return parsedGSOD;
  }

  private static LocalDate parseDate(String date) {
    int year = Integer.parseInt(date.substring(0, 4));
    int month = Integer.parseInt(date.substring(4, 6));
    int day = Integer.parseInt(date.substring(6, 8));
    return LocalDate.of(year, month, day);
  }

  private static float parseMaxMinTemp(String temp) {
    String maxTempTrimmed = temp.endsWith("*") ? temp.substring(0, temp.length() - 1) : temp;
    return Float.parseFloat(maxTempTrimmed);
  }

  private static float parsePrecipitation(String p) {
    // Per documentation, 99.9 suggests no precipitation (different from other attributes where
    // 99... suggests missing data)
    if (p.equals("99.99")) {
      return 0;
    }
    if (p.endsWith("I") || p.endsWith("H")) {
      // Per documentation, I or H flag indicates missing or incomplete data.
      return (float) 99.99;
    }
    // Remove extraneous flags
    int lastIndex = p.length() - 1;
    if (Character.isLetter(p.charAt(lastIndex))) {
      return Float.parseFloat(p.substring(0, lastIndex));
    }
    return Float.parseFloat(p);
  }

  private static float parseSnowDepth(String snow) {
    // Per documentation, 999.9 suggests no snow (different from other attributes where
    // 99... suggests missing data)
    if (snow.equals("999.9")) {
      return 0;
    }
    return (Float.parseFloat(snow));
  }

  private static void parseWeatherIndicators(GSOD_Values gsod, String indicators) {
    PrimitiveIterator.OfInt flags = indicators.chars().iterator();
    gsod.fog = flags.next() == 1;
    gsod.rainOrDrizzle = flags.next() == 1;
    gsod.snowOrIcePellets = flags.next() == 1;
    gsod.hail = flags.next() == 1;
    gsod.thunder = flags.next() == 1;
    gsod.tornadoOrFunnelCloud = flags.next() == 1;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (location == null) {
      throw new IllegalStateException("Cannot emit a GSOD without location data.");
    }
    out.writeInt(date.getYear());
    out.writeInt(date.getMonthValue());
    out.writeInt(date.getDayOfMonth());
    out.writeFloat(meanTempFahrenheit);
    out.writeFloat(dewPointFahrenheit);
    out.writeFloat(seaLevelPressureMillibars);
    out.writeFloat(stationPressureMillibars);
    out.writeFloat(visibilityMiles);
    out.writeFloat(windSpeedKnots);
    out.writeFloat(maxSustainedWindSpeedKnots);
    out.writeFloat(maxWindGustKnots);
    out.writeFloat(maxTempFahrenheit);
    out.writeFloat(minTempFahrenheit);
    out.writeFloat(precipitationInches);
    out.writeFloat(snowDepthInches);
    out.writeBoolean(fog);
    out.writeBoolean(rainOrDrizzle);
    out.writeBoolean(snowOrIcePellets);
    out.writeBoolean(hail);
    out.writeBoolean(thunder);
    out.writeBoolean(tornadoOrFunnelCloud);
    out.writeDouble(location.getLatitude());
    out.writeDouble(location.getLongitude());
  }

  @Override
  public void readFields(DataInput in) throws IOException {

  }
}
