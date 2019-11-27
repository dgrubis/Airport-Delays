package project.helperClasses;

import java.time.LocalDate;

public class Airport {

  private String IATA_Code; // Id code from International Air Transport Association
  private LatLon location;
  private LocalDate date;


  public static Airport parseCSVFromDOT(String record) {
    String[] splitString = record.split(",");
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
}





