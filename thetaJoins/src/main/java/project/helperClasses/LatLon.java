package project.helperClasses;

public class LatLon {
  private final static double AVERAGE_RADIUS_OF_EARTH_MILES = 3958.8;

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

  // Haversine formula adapted from:
  // https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula
  public double distanceInMiles(LatLon other) {
    double lat1 = this.latitude;
    double lon1 = this.longitude;
    double lat2 = other.latitude;
    double lon2 = other.longitude;
    double latDistance = Math.toRadians(lat1 - lat2);
    double lngDistance = Math.toRadians(lon1 - lon2);

    double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
            + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
            * Math.sin(lngDistance / 2) * Math.sin(lngDistance / 2);
    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

    return AVERAGE_RADIUS_OF_EARTH_MILES * c;
  }
}
