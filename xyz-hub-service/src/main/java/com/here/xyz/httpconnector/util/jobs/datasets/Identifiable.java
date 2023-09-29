package com.here.xyz.httpconnector.util.jobs.datasets;

public abstract class Identifiable<T extends Identifiable> extends DatasetDescription {

  private String id;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public T withId(String id) {
    setId(id);
    return (T) this;
  }

  public String getKey() {
    return getId();
  }
}
