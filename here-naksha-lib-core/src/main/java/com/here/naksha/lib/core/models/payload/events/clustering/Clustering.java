package com.here.naksha.lib.core.models.payload.events.clustering;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.naksha.lib.core.models.Typed;

/**
 * The clustering algorithm to apply to the data within the result.
 *
 * <p>If selected, returning the data in a clustered way. This means the data is not necessarily
 * returned in its original shape or with its original properties. Depending on the chosen
 * clustering algorithm, there could be different mandatory and/or optional parameters to specify
 * the behavior of the algorithm.
 */
@JsonSubTypes({@JsonSubTypes.Type(value = ClusteringQuadBin.class), @JsonSubTypes.Type(value = ClusteringHexBin.class)})
public abstract class Clustering implements Typed {}
