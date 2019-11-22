package project.helperClasses;

import java.util.Objects;

/**
 * Represents a unique identifier key for weather stations that provide data to the National Oceanic
 * and Atmospheric Administration (NOAA).
 * <p>
 * USAF = Air Force Station ID.  WBAN = Weather Bureau Air Force Navy number.
 */
public class USAF_WBAN {
  private String USAF; // Documentation warns that USAF may start with a letter
  private int WBAN;

  private USAF_WBAN() {
    this.USAF = "";
    this.WBAN = 0;
  }

  //TODO: finish method
  public static USAF_WBAN parseFromStationCSV(String filePath) {
    return new USAF_WBAN();
  }

  //TODO: finish method
  public static USAF_WBAN parseFromWeatherCSV(String filePath) {
    return new USAF_WBAN();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof USAF_WBAN) {
      USAF_WBAN other = (USAF_WBAN) obj;
      return this.USAF.equals(other.USAF) && this.WBAN == other.WBAN;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.USAF, this.WBAN);
  }
}
