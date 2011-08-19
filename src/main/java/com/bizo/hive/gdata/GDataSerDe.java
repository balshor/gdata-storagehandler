package com.bizo.hive.gdata;

import static com.bizo.hive.gdata.ConfigurationUtils.COLUMN_MAPPING_KEY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class GDataSerDe implements SerDe {
  private final MapWritable cachedWritable = new MapWritable();

  private int fieldCount;
  private StructObjectInspector objectInspector;
  private List<String> columnNames;
  private List<String> row;

  @Override
  public void initialize(final Configuration conf, final Properties tbl) throws SerDeException {
    final String columnString = tbl.getProperty(COLUMN_MAPPING_KEY);
    if (StringUtils.isBlank(columnString)) {
      throw new SerDeException("No column mapping found, use " + COLUMN_MAPPING_KEY);
    }
    final String[] columnNamesArray = columnString.split(",");
    fieldCount = columnNamesArray.length;
    columnNames = new ArrayList<String>(columnNamesArray.length);
    columnNames.addAll(Arrays.asList(columnNamesArray));

    final List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(columnNamesArray.length);
    for (int i = 0; i < columnNamesArray.length; i++) {
      fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }
    objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, fieldOIs);
    row = new ArrayList<String>(columnNamesArray.length);
  }

  @Override
  public Object deserialize(final Writable wr) throws SerDeException {
    if (!(wr instanceof MapWritable)) {
      throw new SerDeException("Expected MapWritable, received " + wr.getClass().getName());
    }

    final MapWritable input = (MapWritable) wr;
    final Text t = new Text();
    row.clear();

    for (int i = 0; i < fieldCount; i++) {
      t.set(columnNames.get(i));
      final Writable value = input.get(t);
      if (value != null && !NullWritable.get().equals(value)) {
        row.add(value.toString());
      } else {
        row.add(null);
      }
    }

    return row;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return objectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return MapWritable.class;
  }

  @Override
  public Writable serialize(final Object obj, final ObjectInspector inspector) throws SerDeException {
    final StructObjectInspector structInspector = (StructObjectInspector) inspector;
    final List<? extends StructField> fields = structInspector.getAllStructFieldRefs();
    if (fields.size() != columnNames.size()) {
      throw new SerDeException(String.format("Required %d columns, received %d.", columnNames.size(), fields.size()));
    }

    cachedWritable.clear();
    for (int c = 0; c < fieldCount; c++) {
      StructField structField = fields.get(c);
      if (structField != null) {
        final Object field = structInspector.getStructFieldData(obj, fields.get(c));
        final ObjectInspector fieldOI = fields.get(c).getFieldObjectInspector();
        final StringObjectInspector fieldStringOI = (StringObjectInspector) fieldOI;
        Writable value = fieldStringOI.getPrimitiveWritableObject(field);
        if (value == null) {
          value = NullWritable.get();
        }
        cachedWritable.put(new Text(columnNames.get(c)), value);
      }
    }
    return cachedWritable;
  }
}
