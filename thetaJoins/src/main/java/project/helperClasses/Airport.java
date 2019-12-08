package project.helperClasses;

import java.time.LocalDate;

public class Airport {
  private static final int FIELD_COUNT = 6;

  private String IATA_Code; // Id code from International Air Transport Association
  private LatLon location;
  private LocalDate date; // Use when joining with dates


  /**
   * Parse an airport record from a Department of Transportation (DOT) record string. Date
   * information will be null (this field is only used for a particular join strategy).
   *
   * @param record an airport input record
   * @return a parsed Airport
   */
  public static Airport parseCSVFromDOT(String record) {
    String[] splitString = record.split(",");
    if (splitString.length < FIELD_COUNT) {
      throw new IllegalArgumentException("Invalid airport with IATA = " + splitString[0]);
    }
    Airport airport = new Airport();
    airport.IATA_Code = splitString[0];
    airport.location = new LatLon(splitString[5], splitString[6]);
    airport.date = null; // Must be added separately or obtained using a different method

    return airport;
  }

  public String getIATA_Code() {
    return IATA_Code;
  }

  public LatLon getLocation() {
    return location;
  }

  public LocalDate getDate() {
    return date;
  }

  @Override
  public String toString() {
    if (date == null) {
      return IATA_Code + "," + location + ",";
    }
    return IATA_Code + "," + location + "," + date + ",";
  }
}





