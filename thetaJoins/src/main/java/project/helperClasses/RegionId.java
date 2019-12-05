package project.helperClasses;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Composite key used for 1-bucket-theta join between flight and weather data.  RegionID is the
 * reducer key, but keys must be secondary sorted on "GSOD < Flight" to reduce memory overhead.
 */
public class RegionId implements WritableComparable<RegionId> {

  private int region;
  private boolean isGSOD;

  public RegionId() {
    this.region = 0;
    this.isGSOD = false;
  }

  public int getRegion() {
    return region;
  }

  public void set(int region, boolean isGSOD) {
    this.region = region;
    this.isGSOD = isGSOD;
  }


  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(region);
    out.writeBoolean(isGSOD);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    region = in.readInt();
    isGSOD = in.readBoolean();
  }

  @Override
  public int compareTo(RegionId other) {
    int cmp = Integer.compare(other.region, this.region);
    if (cmp != 0) {
      return cmp;
    }
    return Boolean.compare(other.isGSOD, this.isGSOD);
  }


  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RegionId) {
      RegionId second = (RegionId) obj;
      return compareTo(second) == 0;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(region, isGSOD);
  }
}
