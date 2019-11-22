package project.helperClasses;

public class LatLong {
  private double latitude;
  private double longitude;

  public LatLong(double latitude, double longitude) {
    this.latitude = latitude;
    this.longitude = longitude;
  }

  public LatLong(String latitude, String longitude) {
    this.latitude = Double.parseDouble(latitude);
    this.longitude = Double.parseDouble(longitude);
  }
}
