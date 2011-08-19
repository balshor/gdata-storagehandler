gdata-storagehandler

This project implements a HiveStorageHandler that allows Hive to read and write data from a Google spreadsheet.

Although Hive/Hadoop are geared towards processing big data, this storage handler implementation is geared towards 
"Small Data".  The original use case was for writing around a dozen lines of data containing the final output of a 
report into a Google spreadsheet.

Because of the small data orientation, it is recommended to read or write data from tables backed by this StorageHandler
from only a single mapper or reducer.  Using multiple mappers or reducers can result in duplicate data being read or
written to the spreadsheet.

Some other notes:

* The spreadsheet must be writable by the specified user.
* We use 2-legged OAuth.  See http://code.google.com/apis/gdata/docs/auth/oauth.html#2LeggedOAuth and 
  http://www.google.com/support/a/bin/answer.py?hl=en&answer=162105. 
* The spreadsheet must exist and have the correct headers.  Any Hive columns that do not map to a column header will not
  be written to the spreadsheet.  
* All writes are appends.

Sample usage:

    add jar gdata-storagehandler.jar ;

    create external table output(day string, cnt int, source_class string, source_method string, thrown_class string)
    stored by 'com.bizo.hive.gdata.GDataStorageHandler'
    with serdeproperties (
      "gdata.user" = "user@bizo.com",
      "gdata.consumer.key" = "bizo.com",
      "gdata.consumer.secret" = "...",
      "gdata.spreadsheet.name" = "Daily Exception Summary",
      "gdata.worksheet.name" = "First Worksheet",
      "gdata.columns.mapping" = "day,count,class,method,thrown"
    )
    ;

If you are using Amazon's Elastic Mapreduce, you can add the jar file as follows:

    add jar s3://com-bizo-public/hive/storagehandler/gdata-storagehandler-0.1.jar ;
