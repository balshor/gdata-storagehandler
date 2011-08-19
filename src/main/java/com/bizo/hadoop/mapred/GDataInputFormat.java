package com.bizo.hadoop.mapred;

import static com.bizo.hive.gdata.ConfigurationUtils.readCredentials;
import static com.bizo.hive.gdata.ConfigurationUtils.spreadsheetName;
import static com.bizo.hive.gdata.ConfigurationUtils.worksheetName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.*;

import com.bizo.util.gdata.GDataService;
import com.google.gdata.data.spreadsheet.ListFeed;

/**
 * An input format for reading data out of a Google spreadsheet.
 */
@SuppressWarnings("deprecation")
public class GDataInputFormat extends HiveInputFormat<LongWritable, MapWritable> {
  @Override
  public RecordReader<LongWritable, MapWritable> getRecordReader(
      final InputSplit split,
      final JobConf conf,
      final Reporter reporter) throws IOException {
    if (!(split instanceof GDataInputSplit)) {
      // should never happen since getSplits returns GDataInputSplits
      throw new RuntimeException("Incompatible split type " + split.getClass().getName() + ", required GDataInputSplit");
    }
    return new GDataRecordReader(readCredentials(conf), (GDataInputSplit) split, spreadsheetName(conf), worksheetName(conf));
  }

  @Override
  public InputSplit[] getSplits(final JobConf conf, final int numSplits) throws IOException {
    final String spreadsheetName = spreadsheetName(conf);
    final String worksheetName = worksheetName(conf);
    final ListFeed lines = new GDataService(readCredentials(conf)).getWorksheet(spreadsheetName, worksheetName);
    if (lines == null) {
      throw new IOException(String.format(
        "Could not find worksheet [%s] in spreadsheet [%s].",
        worksheetName,
        spreadsheetName));
    }
    final int totalResults = lines.getTotalResults();
    final int splitSize = totalResults / numSplits;

    final List<InputSplit> splits = new ArrayList<InputSplit>();

    final Path[] tablePaths = FileInputFormat.getInputPaths(conf);

    // Split the rows into n-number of chunks and adjust the last chunk
    // accordingly
    for (int i = 0; i < numSplits; i++) {
      GDataInputSplit split;

      if ((i + 1) == numSplits) {
        split = new GDataInputSplit(i * splitSize, totalResults, tablePaths[0]);
      } else {
        split = new GDataInputSplit(i * splitSize, (i + 1) * splitSize, tablePaths[0]);
      }

      splits.add(split);
    }

    return splits.toArray(new InputSplit[splits.size()]);
  }

}
