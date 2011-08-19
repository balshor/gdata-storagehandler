package com.bizo.hive.gdata;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.mapred.JobConf;

import com.bizo.util.gdata.GDataCredentials;
import com.google.common.collect.ImmutableSet;

/** Utilities for managing the serde properties supported by the gdata storage handler. */
@SuppressWarnings("deprecation")
public final class ConfigurationUtils {
  private ConfigurationUtils() {
  }

  public static final String USER_KEY = "gdata.user";
  public static final String CONSUMER_KEY_KEY = "gdata.consumer.key";
  public static final String CONSUMER_SECRET_KEY = "gdata.consumer.secret";

  public static final String SPREADSHEET_NAME_KEY = "gdata.spreadsheet.name";
  public static final String WORKSHEET_NAME_KEY = "gdata.worksheet.name";
  public static final String DEFAULT_WORKSHEET_NAME = "Sheet1";

  public static final String COLUMN_MAPPING_KEY = "gdata.columns.mapping";

  public static final Set<String> REQUIRED_PROPERTIES = ImmutableSet.of(SPREADSHEET_NAME_KEY, COLUMN_MAPPING_KEY);
  public static final Set<String> ALL_PROPERTIES = ImmutableSet.of(
    USER_KEY,
    CONSUMER_KEY_KEY,
    CONSUMER_SECRET_KEY,
    SPREADSHEET_NAME_KEY,
    COLUMN_MAPPING_KEY,
    WORKSHEET_NAME_KEY);

  public static GDataCredentials readCredentials(JobConf conf) {
    final String user = conf.get(USER_KEY);
    final String consumerKey = conf.get(CONSUMER_KEY_KEY);
    final String consumerSecret = conf.get(CONSUMER_SECRET_KEY);
    return new GDataCredentials(user, consumerKey, consumerSecret);
  }

  public static void copyGDataProperties(Properties from, Map<String, String> to) {
    for (String key : ALL_PROPERTIES) {
      String value = from.getProperty(key);
      if (value != null) {
        to.put(key, value);
      }
    }
  }

  public static Set<String> missingProperties(final JobConf conf) {
    final Set<String> toReturn = new HashSet<String>();
    for (final String property : REQUIRED_PROPERTIES) {
      if (conf.get(property) == null) {
        toReturn.add(property);
      }
    }
    return toReturn;
  }

  public static String spreadsheetName(final JobConf conf) {
    final String name = conf.get(SPREADSHEET_NAME_KEY);
    if (name == null) {
      throw new IllegalArgumentException("No spreadsheet name specified -- use gdata.spreadsheet.name");
    }
    return name;
  }

  public static String worksheetName(final JobConf conf) {
    return conf.get(WORKSHEET_NAME_KEY, DEFAULT_WORKSHEET_NAME);
  }

}
