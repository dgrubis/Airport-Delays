package project.helperClasses;

/**
 * Represents a National Climatic Data Center (NCDC) weather station.  Contains identification
 * information and latitude-longitude location.
 */
public class WeatherStation {
  private String USAF_WBAN; // Air Force Station ID and Weather Bureau Air Force Navy number
  private LatLon location;

  /**
   * Create a WeatherStation by parsing a raw weather station record from the National Oceanic and
   * Atmospheric Administration (NOAA).
   *
   * @param record a NOAA weather station record.
   */
  public WeatherStation(String record) {
    String[] station = record.split(",");
    USAF_WBAN = station[1] + station[2];
    if (station.length < 9 || station[7].equals("") || station[8].equals("")) {
      location = null;
    } else {
      location = new LatLon(station[7], station[8]);
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


