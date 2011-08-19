package com.bizo.hadoop.mapred;

import static com.bizo.hive.gdata.ConfigurationUtils.missingProperties;
import static com.bizo.hive.gdata.ConfigurationUtils.readCredentials;
import static com.bizo.hive.gdata.ConfigurationUtils.spreadsheetName;
import static com.bizo.hive.gdata.ConfigurationUtils.worksheetName;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.Progressable;

import com.google.common.base.Joiner;

/**
 * Defines the output format to be that written by a GDataRecordWriter.
 * 
 * @author darren
 * 
 */
@SuppressWarnings("deprecation")
public class GDataOutputFormat implements OutputFormat<NullWritable, MapWritable>,
    HiveOutputFormat<NullWritable, MapWritable> {
  private static final Logger LOGGER = Logger.getLogger(GDataOutputFormat.class.getName());

  @Override
  public void checkOutputSpecs(final FileSystem ignored, final JobConf conf) throws IOException {
    final Set<String> missingProperties = missingProperties(conf);
    if (!missingProperties.isEmpty()) {
      String msg = "Configuration is missing required properties: " + Joiner.on(", ").join(missingProperties);
      LOGGER.info(msg);
      throw new RuntimeException(msg);
    }
  }

  @Override
  public org.apache.hadoop.mapred.RecordWriter<NullWritable, MapWritable> getRecordWriter(
      final FileSystem ignored,
      final JobConf conf,
      final String name,
      final Progressable progress) throws IOException {
    throw new RuntimeException("Error: Hive should not invoke this method.");
  }

  @Override
  public RecordWriter getHiveRecordWriter(
      JobConf conf,
      Path finalOutPath,
      Class<? extends Writable> valueClass,
      boolean isCompressed,
      Properties tableProperties,
      Progressable progress) throws IOException {
    return new GDataRecordWriter(readCredentials(conf), spreadsheetName(conf), worksheetName(conf));
  }
}
