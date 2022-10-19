package com.here.xyz.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "RevisionEvent")
public class RevisionEvent extends Event<RevisionEvent> {
  private Operation operation;
  private PropertyQuery revision;

  public Operation getOperation() {
    return operation;
  }

  public void setOperation(Operation operation) {
    this.operation = operation;
  }

  public RevisionEvent withOperation(Operation operation) {
    setOperation(operation);
    return this;
  }

  public PropertyQuery getRevision() {
    return revision;
  }

  public void setRevision(PropertyQuery revision) {
    this.revision = revision;
  }

  public RevisionEvent withRevision(PropertyQuery revision) {
    setRevision(revision);
    return this;
  }

  public enum Operation {
    DELETE
  }
}
