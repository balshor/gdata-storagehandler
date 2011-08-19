package com.bizo.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

/**
 * Describes a range of rows in a Google spreadsheet.
 */
@SuppressWarnings("deprecation")
public class GDataInputSplit extends FileSplit implements InputSplit {
  private static final String[] EMPTY_ARRAY = new String[] {};

  private long start = 0;
  private long end = 0;

  // Note: for deserialization purposes only.  Having a null path will cause runtime exceptions.
  public GDataInputSplit() {
    super((Path) null, 0, 0, EMPTY_ARRAY);
  }

  public GDataInputSplit(final long start, final long end, final Path dummypath) {
    super(dummypath, 0, 0, EMPTY_ARRAY);
    this.start = start;
    this.end = end;
  }

  @Override
  public void readFields(final DataInput input) throws IOException {
    super.readFields(input);
    start = input.readLong();
    end = input.readLong();
  }

  @Override
  public void write(final DataOutput output) throws IOException {
    super.write(output);
    output.writeLong(start);
    output.writeLong(end);
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public void setStart(long start) {
    this.start = start;
  }

  public void setEnd(long end) {
    this.end = end;
  }

  @Override
  public long getLength() {
    return end - start;
  }

  /* Data is remote for all nodes. */
  @Override
  public String[] getLocations() throws IOException {
    return EMPTY_ARRAY;
  }

  @Override
  public String toString() {
    return String.format("GDataInputSplit(start=%s,end=%s)", start, end);
  }
}
