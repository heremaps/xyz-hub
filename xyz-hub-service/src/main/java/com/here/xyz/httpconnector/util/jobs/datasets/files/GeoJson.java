package com.here.xyz.httpconnector.util.jobs.datasets.files;

import static com.here.xyz.httpconnector.util.jobs.datasets.files.GeoJson.EntityPerLine.Feature;
import static com.here.xyz.httpconnector.util.jobs.datasets.files.GeoJson.JsonMultiLineStandard.NEW_LINE;

public class GeoJson extends FileFormat {
  private EntityPerLine entityPerLine = Feature;
  private JsonMultiLineStandard multiLineStandard = NEW_LINE;

  public EntityPerLine getEntityPerLine() {
    return entityPerLine;
  }

  public void setEntityPerLine(EntityPerLine entityPerLine) {
    this.entityPerLine = entityPerLine;
  }

  public GeoJson withEntityPerLine(EntityPerLine entityPerLine) {
    setEntityPerLine(entityPerLine);
    return this;
  }

  public JsonMultiLineStandard getMultiLineStandard() {
    return multiLineStandard;
  }

  public void setMultiLineStandard(JsonMultiLineStandard multiLineStandard) {
    this.multiLineStandard = multiLineStandard;
  }

  public GeoJson withMultiLineStandard(JsonMultiLineStandard multiLineStandard) {
    setMultiLineStandard(multiLineStandard);
    return this;
  }

  public enum EntityPerLine {
    Feature,
    FeatureCollection
  }

  public enum JsonMultiLineStandard {
    RFC7464,
    NEW_LINE
  }
}
