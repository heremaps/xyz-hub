package com.here.xyz.models.payload.events.clustering;

import static com.here.xyz.util.json.JsonSerializable.format;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.exceptions.ParameterError;
import com.here.xyz.models.payload.events.Sampling;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The "hexbin" algorithm divides the world in hexagonal "bins" on a specified resolution. Each
 * hexagon has an address described by the H3 addressing scheme. For more information on this topic
 * see: <a href="https://eng.uber.com/h3/">https://eng.uber.com/h3/</a>.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "HexBinClustering")
public class ClusteringHexBin extends Clustering {

  public static final int MIN_RESOLUTION = 0;
  public static final int MAX_RESOLUTION = 13;

  /**
   * The optional H3 hexagon resolution [0,13]. This parameter is as well available with the
   * downward compatible naming “resolution”.
   */
  private @Nullable Integer absoluteResolution;

  public @Nullable Integer getAbsoluteResolution() {
    if (absoluteResolution != null
        && (absoluteResolution < MIN_RESOLUTION || absoluteResolution > MAX_RESOLUTION)) {
      return null;
    }
    return absoluteResolution;
  }

  public void setAbsoluteResolution(@Nullable Integer absoluteResolution) throws ParameterError {
    if (absoluteResolution != null
        && (absoluteResolution < MIN_RESOLUTION || absoluteResolution > MAX_RESOLUTION)) {
      throw new ParameterError(
          format(
              "absoluteResolution must be %d to %d and was: %d",
              MIN_RESOLUTION, MAX_RESOLUTION, absoluteResolution));
    }
    this.absoluteResolution = absoluteResolution;
  }

  /** Optional value to be added to current used resolution [-2,2]. */
  private @Nullable Integer relativeResolution;

  public @Nullable Integer getRelativeResolution() {
    if (relativeResolution != null && (relativeResolution < -2 || relativeResolution > 2)) {
      return null;
    }
    return relativeResolution;
  }

  public void setRelativeResolution(@Nullable Integer relativeResolution) throws ParameterError {
    if (relativeResolution != null && (relativeResolution < -2 || relativeResolution > 2)) {
      throw new ParameterError("relativeResolution must be -2 to 2 and was: " + relativeResolution);
    }
    this.relativeResolution = relativeResolution;
  }

  /** A property of the original features for which to calculate statistics. */
  public @Nullable String property;

  /** Return the centroid of hexagons as geojson feature. */
  public boolean pointMode;

  /** Force to evaluate the first object coordinate only (default: false). */
  public boolean singleCoord;

  /** Sampling ratio of underlying dataset. */
  public @NotNull Sampling sampling = Sampling.OFF;
}
