package com.bizo.hadoop.mapred;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import com.bizo.util.gdata.GDataCredentials;
import com.bizo.util.gdata.GDataService;
import com.google.gdata.client.spreadsheet.SpreadsheetService;
import com.google.gdata.data.spreadsheet.CustomElementCollection;
import com.google.gdata.data.spreadsheet.ListEntry;
import com.google.gdata.util.ServiceException;

public class GDataRecordWriter implements RecordWriter {
  private final GDataCredentials credentials;
  private final String spreadsheetName;
  private final String worksheetName;

  public GDataRecordWriter(final GDataCredentials credentials, final String spreadsheetName, final String worksheetName) {
    this.credentials = credentials;
    this.spreadsheetName = spreadsheetName;
    this.worksheetName = worksheetName;
  }

  @Override
  public void close(boolean abort) throws IOException {
    // noop
  }

  @Override
  public void write(Writable w) throws IOException {
    MapWritable map = (MapWritable) w;

    final GDataService supplier = new GDataService(credentials);
    final SpreadsheetService service = supplier.getSpreadsheetService();
    final URL worksheetURL = supplier.getWorksheetURL(spreadsheetName, worksheetName);
    if (worksheetURL == null) {
      throw new IOException(String.format(
        "Could not find worksheet [%s] in spreadsheet [%s].",
        worksheetName,
        spreadsheetName));
    }

    final ListEntry newEntry = new ListEntry();
    final CustomElementCollection customElements = newEntry.getCustomElements();
    for (final Map.Entry<Writable, Writable> entry : map.entrySet()) {
      customElements.setValueLocal(entry.getKey().toString(), entry.getValue().toString());
    }

    try {
      service.insert(worksheetURL, newEntry);
    } catch (final ServiceException e) {
      throw new RuntimeException(e);
    }
  }
}
