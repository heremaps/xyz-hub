package com.here.xyz.httpconnector.util.jobs.datasets.files;

import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.JSON_WKB;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.PARTITIONED_JSON_WKB;
import static com.here.xyz.httpconnector.util.jobs.datasets.files.Csv.JsonColumnEncoding.BASE64;
import static com.here.xyz.httpconnector.util.jobs.datasets.files.GeoJson.EntityPerLine.FeatureCollection;

import com.here.xyz.httpconnector.util.jobs.Job.CSVFormat;
import com.here.xyz.httpconnector.util.jobs.datasets.files.GeoJson.EntityPerLine;

/**
 * @deprecated This format should not be used in any public API. It's rather kept for some internal purposes to keep BWC.
 */
@Deprecated
public class Csv extends FileFormat {
  private EntityPerLine entityPerLine = FeatureCollection;
  private JsonColumnEncoding encoding = BASE64;
  private boolean addPartitionKey;

  public EntityPerLine getEntityPerLine() {
    return entityPerLine;
  }

  public void setEntityPerLine(EntityPerLine entityPerLine) {
    this.entityPerLine = entityPerLine;
  }

  public Csv withEntityPerLine(EntityPerLine entityPerLine) {
    setEntityPerLine(entityPerLine);
    return this;
  }

  public JsonColumnEncoding getEncoding() {
    return encoding;
  }

  public void setEncoding(JsonColumnEncoding encoding) {
    this.encoding = encoding;
  }

  public Csv withEncoding(JsonColumnEncoding encoding) {
    setEncoding(encoding);
    return this;
  }

  public boolean isAddPartitionKey() {
    return addPartitionKey;
  }

  public void setAddPartitionKey(boolean addPartitionKey) {
    this.addPartitionKey = addPartitionKey;
  }

  public CSVFormat toBWCFormat() {
    return isAddPartitionKey() ? PARTITIONED_JSON_WKB : JSON_WKB;
  }

  public Csv withAddPartitionKey(boolean addPartitionKey) {
    setAddPartitionKey(addPartitionKey);
    return this;
  }

  public enum JsonColumnEncoding {
    BASE64
  }
}
