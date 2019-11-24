package project.helperClasses.gsod;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import project.helperClasses.LatLon;

public interface GSOD {

  public String getUSAF();

  public String getUSAF_WBAN();

  public void setLocation(LatLon location);

  public void write(DataOutput out) throws IOException;

  public void readFields(DataInput in) throws IOException;

}
