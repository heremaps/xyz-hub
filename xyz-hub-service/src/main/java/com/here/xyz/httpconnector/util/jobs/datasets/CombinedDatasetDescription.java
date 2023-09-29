package com.here.xyz.httpconnector.util.jobs.datasets;

import java.util.List;

/**
 * A {@link DatasetDescription} which is a logical representation of multiple Sub-DatasetDescriptions being combined.
 * @param <T> The type of the Sub-DatasetDescriptions
 */
public interface CombinedDatasetDescription<T> {

  /**
   * Creates the Sub-DatasetDescriptions this CombinedDatasetDescription is representing.
   * @return The list of Sub-DatasetDescription instances
   */
  List<T> createChildEntities();
}
