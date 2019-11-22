package project.helperClasses.gsod;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import project.helperClasses.LatLong;

public interface GSOD {

  public String getUSAF_WBAN();

  public void setLocation(LatLong location);

  public void write(DataOutput out) throws IOException;

  public void readFields(DataInput in) throws IOException;

}
