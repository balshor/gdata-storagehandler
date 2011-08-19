package com.bizo.hive.gdata;

import static com.bizo.hive.gdata.ConfigurationUtils.copyGDataProperties;

import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

import com.bizo.hadoop.mapred.GDataInputFormat;
import com.bizo.hadoop.mapred.GDataOutputFormat;

@SuppressWarnings("deprecation")
public class GDataStorageHandler implements HiveStorageHandler {
  private Configuration conf;

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(final Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void configureTableJobProperties(final TableDesc tableDesc, final Map<String, String> jobProperties) {
    Properties properties = tableDesc.getProperties();
    copyGDataProperties(properties, jobProperties);
  }

  // Note: return a HiveInputFormat, which is also a Hadoop InputFormat.
  @SuppressWarnings("rawtypes")
  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return GDataInputFormat.class;
  }

  // Note: return a HiveOutputFormat, not a Hadoop OutputFormat.
  @SuppressWarnings("rawtypes")
  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return GDataOutputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return GDataSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return null;
  }
}
