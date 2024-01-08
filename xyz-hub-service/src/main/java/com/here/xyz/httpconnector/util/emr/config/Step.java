package com.here.xyz.httpconnector.util.emr.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.xyz.Typed;
import com.here.xyz.httpconnector.util.emr.config.Step.ConvertToGeoparquet;
import com.here.xyz.httpconnector.util.emr.config.Step.ReadFeaturesCSV;
import com.here.xyz.httpconnector.util.emr.config.Step.ReplaceWkbWithGeo;
import com.here.xyz.httpconnector.util.emr.config.Step.WriteGeoparquet;
import java.util.List;

@JsonSubTypes({
    @JsonSubTypes.Type(value = ReadFeaturesCSV.class, name = "ReadFeaturesCSV"),
    @JsonSubTypes.Type(value = ReplaceWkbWithGeo.class, name = "ReplaceWkbWithGeo"),
    @JsonSubTypes.Type(value = ConvertToGeoparquet.class, name = "ConvertToGeoparquet"),
    @JsonSubTypes.Type(value = WriteGeoparquet.class, name = "WriteGeoparquet")
})
public abstract class Step implements Typed {
  public static class ReadFeaturesCSV extends Step {
    private String inputDirectory;
    private List<CsvColumns> columns;

    public String getInputDirectory() {
      return inputDirectory;
    }

    public void setInputDirectory(String inputDirectory) {
      this.inputDirectory = inputDirectory;
    }

    public ReadFeaturesCSV withInputDirectory(String inputDirectory) {
      setInputDirectory(inputDirectory);
      return this;
    }

    public List<CsvColumns> getColumns() {
      return columns;
    }

    public void setColumns(List<CsvColumns> columns) {
      this.columns = columns;
    }

    public ReadFeaturesCSV withColumns(List<CsvColumns> columns) {
      setColumns(columns);
      return this;
    }
    public enum CsvColumns {
      JSON_DATA,
      WKB
    }
  }

  public static class ReplaceWkbWithGeo extends Step {}

  public static class ConvertToGeoparquet extends Step {}

  public static class WriteGeoparquet extends Step {
    private String outputDirectory;

    public String getOutputDirectory() {
      return outputDirectory;
    }

    public void setOutputDirectory(String outputDirectory) {
      this.outputDirectory = outputDirectory;
    }

    public WriteGeoparquet withOutputDirectory(String outputDirectory) {
      setOutputDirectory(outputDirectory);
      return this;
    }
  }
}
