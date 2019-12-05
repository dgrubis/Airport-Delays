package project.helperClasses;

/**
 * Represents a National Climatic Data Center (NCDC) weather station.  Contains identification
 * information and latitude-longitude location.
 */
public class WeatherStation {
  public static final String DEFAULT_WBAN = "99999";

  private static final int USAF = 1;
  private static final int WBAN = 2;
  private static final int LAT = 7;
  private static final int LON = 8;
  private static final int MIN_FIELDS = 9;

  private String USAF_WBAN; // Air Force Station ID and Weather Bureau Air Force Navy number
  private LatLon location;

  /**
   * Create a WeatherStation by parsing a raw weather station record from the National Oceanic and
   * Atmospheric Administration (NOAA).
   *
   * @param record a NOAA weather station record.
   */
  public WeatherStation(String record) {
    String[] station = record.split(",\\s*");

    // NOAA stations data may truncate starting 0's for 5-digit WBAN's.
    StringBuilder WBAN_Value = new StringBuilder(station[WBAN]);
    while (WBAN_Value.length() < 5) {
      WBAN_Value.insert(0, "0");
    }

    USAF_WBAN = station[USAF] + "_" + WBAN_Value;
    if (station.length < MIN_FIELDS || station[LAT].equals("") || station[LON].equals("")) {
      location = null;
    } else {
      location = new LatLon(station[LAT], station[LON]);
    }
  }

  public String getUSAF_WBAN() {
    return USAF_WBAN;
  }

  public LatLon getLocation() {
    return location;
  }

  public boolean isValid() {
    return location != null;
  }
}


