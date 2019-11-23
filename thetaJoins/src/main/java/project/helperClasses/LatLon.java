package project.helperClasses;

public class LatLon {
  private double latitude;
  private double longitude;

  public LatLon(double latitude, double longitude) {
    this.latitude = latitude;
    this.longitude = longitude;
  }

  public LatLon(String latitude, String longitude) {
    this.latitude = Double.parseDouble(latitude);
    this.longitude = Double.parseDouble(longitude);
  }

  public double getLatitude() {
    return latitude;
  }

  public double getLongitude() {
    return longitude;
  }

  @Override
  public String toString() {
    return latitude + "," + longitude;
  }
}
