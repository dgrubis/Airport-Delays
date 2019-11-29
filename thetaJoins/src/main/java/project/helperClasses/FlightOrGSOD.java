package project.helperClasses;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import project.helperClasses.gsod.GSOD;
import project.helperClasses.gsod.GSOD_Text;

/**
 * Wrapper class that represents either a Flight or a GSOD. Used to pass both Flights and GSODs to
 * the same reducer.
 */
public class FlightOrGSOD implements Writable {
  private Text data;
  private boolean isFlight;

  public boolean isFlight() {
    return isFlight;

  }

  public Flight getFlight() {
    if (!isFlight()) {
      throw new IllegalStateException("Cannot generate flight data. This instance is a GSOD record");
    }
    return Flight.parseCSVWithLatLon(data.toString());
  }

  public GSOD getGSOD() {
    if (isFlight()) {
      throw new IllegalStateException("Cannot generate GSOD data. This instance is a Flight record");
    }
    return GSOD_Text.parseCSVWithLatLon(data.toString());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(isFlight);
    data.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    isFlight = in.readBoolean();
    data.readFields(in);
  }
}
