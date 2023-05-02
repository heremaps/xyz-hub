package com.here.xyz.models.geojson.implementation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.Extensible;
import com.here.xyz.Typed;
import com.here.xyz.View.All;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract feature that allows a simple GeoJson compatible feature to be implemented.
 *
 * @param <PROPERTIES> The properties type.
 * @param <SELF>       This type.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public abstract class AbstractFeature<
    PROPERTIES extends Extensible<PROPERTIES>,
    SELF extends AbstractFeature<PROPERTIES, SELF>>
    extends Extensible<SELF> implements Typed {

  @JsonProperty
  @JsonView(All.class)
  private String id;

  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_NULL)
  private BBox bbox;

  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_NULL)
  private Geometry geometry;

  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_NULL)
  private PROPERTIES properties;

  /**
   * Create a new empty feature.
   */
  public AbstractFeature() {
  }

  /**
   * Creates new empty properties.
   *
   * @return new empty properties.
   */
  abstract protected @NotNull PROPERTIES newProperties();

  public @Nullable String getId() {
    return id;
  }

  public void setId(@Nullable String id) {
    this.id = id;
  }

  public @NotNull SELF withId(@Nullable String id) {
    setId(id);
    return self();
  }

  public @Nullable BBox getBbox() {
    return bbox;
  }

  public void setBbox(@Nullable BBox bbox) {
    this.bbox = bbox;
  }

  public @NotNull SELF withBbox(@Nullable BBox bbox) {
    setBbox(bbox);
    return self();
  }

  public @Nullable Geometry getGeometry() {
    return geometry;
  }

  public void setGeometry(@Nullable Geometry geometry) {
    this.geometry = geometry;
  }

  public @NotNull SELF withGeometry(@Nullable Geometry geometry) {
    setGeometry(geometry);
    return self();
  }

  public @Nullable PROPERTIES getProperties() {
    return properties;
  }

  public void setProperties(@Nullable PROPERTIES properties) {
    this.properties = properties;
  }

  public @NotNull SELF withProperties(@Nullable PROPERTIES properties) {
    setProperties(properties);
    return self();
  }

  public @NotNull PROPERTIES useProperties() {
    if (properties == null) {
      properties = newProperties();
    }
    return properties;
  }

  public void calculateAndSetBbox(boolean recalculateBBox) {
    if (!recalculateBBox && getBbox() != null) {
      return;
    }

    final Geometry geometry = getGeometry();
    if (geometry == null) {
      setBbox(null);
    } else {
      setBbox(geometry.calculateBBox());
    }
  }

  /**
   * Validates the geometry of the feature and throws an exception if the geometry is invalid. This method will not throw an exception if
   * the geometry is missing, so null or undefined, but will do so, when the geometry is somehow broken.
   *
   * @throws InvalidGeometryException if the geometry is invalid.
   */
  public void validateGeometry() throws InvalidGeometryException {
    final Geometry geometry = getGeometry();
    if (geometry == null) {
      // This is valid, the feature simply does not have any geometry.
      return;
    }

    if (geometry instanceof GeometryCollection) {
      throw new InvalidGeometryException("GeometryCollection is not supported.");
    }

    geometry.validate();
  }
}
