package project.helperClasses;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDate;

/**
 * Global surface summary of the day produced by the National Climatic Data Center (NCDC) and
 * available through the National Oceanic and Atmospheric Administration (NOAA).  An instance of
 * this class represents the GSOD data from a particular weather station on a particular day.
 */
public class GSOD implements Writable {
  // TODO: consider using floats here?  Initial input data doesn't exceed two decimal places.
  private String USAF_WBAN; // Air Force Station ID and Weather Bureau Air Force Navy number
  private LocalDate date;
  private double meanTempFahrenheit;
  private double maxTempFahrenheit;  // TODO: Do we care about max and min temp, or just mean?
  private double minTempFahrenheit;
  private double dewPointFahrenheit;
  private double seaLevelPressureMillibars;
  private double stationPressureMillibars;
  private double visibilityMiles;
  private double windSpeedKnots;
  private double maxSustainedWindSpeedKnots;
  private double maxWindGustKnots;
  private double precipitationInches; // Total rain and/or melted snow
  private double snowDepthInches;
  private boolean fog;
  private boolean rainOrDrizzle;
  private boolean snowOrIcePellets;
  private boolean hail;
  private boolean thunder;
  private boolean tornadoOrFunnelCloud;
  private LatLong location; // Must be provided from another dataset

  public String getUSAF_WBAN() {
    return USAF_WBAN;
  }

  public void setLocation(LatLong location) {
    this.location = location;
  }

  // TODO
  public static GSOD parseCSV(String record) {
    GSOD parsedGSOD = new GSOD();
    return parsedGSOD;
  }

  // TODO
  @Override
  public void write(DataOutput out) throws IOException {

  }

  // TODO
  @Override
  public void readFields(DataInput in) throws IOException {

  }
}
