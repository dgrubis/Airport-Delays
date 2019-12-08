package project.helperClasses;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class FRSHTT_Separator {

  public static void main(final String[] args) throws FileNotFoundException, IOException {
    BufferedReader reader = new BufferedReader(new FileReader("thetaJoins/FinalOutput"));
    BufferedWriter writer = new BufferedWriter(new FileWriter("thetaJoins/outPutFlights.csv"));

    while (true) {
      String record = reader.readLine();
      if (record == null) {
        break;
      }

      Flight flight = Flight.parseCSVWithLatLonWeather(record);
      if (flight.getOriginGSOD() != null) {
        flight.getOriginGSOD().splitFRSHTT();
      }
      if (flight.getDestGSOD() != null) {
        flight.getDestGSOD().splitFRSHTT();
      }
      writer.write(flight.toString() + "\n");
    }
    reader.close();
    writer.close();
  }

}
