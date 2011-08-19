package com.bizo.hadoop.mapred;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.RecordReader;

import com.bizo.util.gdata.GDataCredentials;
import com.bizo.util.gdata.GDataService;
import com.google.gdata.data.spreadsheet.ListEntry;
import com.google.gdata.data.spreadsheet.ListFeed;

/**
 * Reads records from a Google Spreadsheet.
 * 
 * Note that due to limitations in the Google spreadsheet API, each reader will actually download the entire
 * spreadsheet.
 */
public class GDataRecordReader implements RecordReader<LongWritable, MapWritable> {

  private static final String SERIALIZED_NULL = NullWritable.get().toString();
  private final GDataInputSplit split;
  private final Iterator<ListEntry> lines;
  private int pos = 0;

  public GDataRecordReader(
      final GDataCredentials credentials,
      final GDataInputSplit split,
      final String spreadsheetName,
      final String worksheetName) {
    final ListFeed worksheetFeed = new GDataService(credentials).getWorksheet(spreadsheetName, worksheetName);
    final List<ListEntry> linesList = worksheetFeed.getEntries();
    if (linesList == null) {
      throw new RuntimeException(String.format(
        "Could not find worksheet [%s] in spreadsheet [%s].",
        worksheetName,
        spreadsheetName));
    }
    this.lines = linesList.subList((int) split.getStart(), (int) split.getEnd()).iterator();
    this.split = split;
  }

  @Override
  public LongWritable createKey() {
    return new LongWritable();
  }

  @Override
  public MapWritable createValue() {
    return new MapWritable();
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public float getProgress() throws IOException {
    return split.getLength() > 0 ? pos / (float) split.getLength() : 1.0f;
  }

  @Override
  public void close() throws IOException {
    // noop
  }

  @Override
  public boolean next(final LongWritable keyHolder, final MapWritable valueHolder) throws IOException {
    if (!lines.hasNext()) {
      return false;
    }

    keyHolder.set(pos);
    pos++;

    final ListEntry entry = lines.next();
    valueHolder.clear();
    for (final String tag : entry.getCustomElements().getTags()) {
      final String value = entry.getCustomElements().getValue(tag);
      final Writable writableValue = SERIALIZED_NULL.equals(value) ? NullWritable.get() : new Text(value);
      valueHolder.put(new Text(tag), writableValue);
    }

    return true;
  }

}
