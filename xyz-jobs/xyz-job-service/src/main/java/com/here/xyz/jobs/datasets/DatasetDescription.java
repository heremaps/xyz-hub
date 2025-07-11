/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */

package com.here.xyz.jobs.datasets;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_DEFAULT;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.Typed;
import com.here.xyz.jobs.datasets.DatasetDescription.Map;
import com.here.xyz.jobs.datasets.DatasetDescription.Space;
import com.here.xyz.jobs.datasets.filters.FilteringSource;
import com.here.xyz.jobs.datasets.filters.Filters;
import com.here.xyz.jobs.datasets.streams.Notifications;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space.Extension;
import com.here.xyz.util.geo.GeometryValidator;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.ErrorResponseException;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.util.HashSet;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = Map.class, name = "Map"),
    @JsonSubTypes.Type(value = Space.class, name = "Space"),
    @JsonSubTypes.Type(value = Spaces.class, name = "Spaces"),
    @JsonSubTypes.Type(value = Files.class, name = "Files"),
    @JsonSubTypes.Type(value = Notifications.class, name = "Notifications"),
    @JsonSubTypes.Type(value = Datasets.class, name = "Datasets")
})
@JsonInclude(NON_DEFAULT)
public abstract class DatasetDescription implements Typed {

  /**
   * @return the (primary) key of this DatasetDescription to search for it in the persistence layer.
   *  Returning null means that any other key is matched.
   */
  @JsonIgnore
  public abstract String getKey();

  @JsonIgnore
  public Set<String> getResourceKeys() {
    String primaryKey = getKey();
    return primaryKey == null ? Set.of() : Set.of(primaryKey);
  }

  public static class Map extends Identifiable implements VersionedSource<Map> {
    private Ref versionRef = new Ref(Ref.HEAD);

    @Override
    public Ref getVersionRef() {
      return versionRef;
    }

    @Override
    public void setVersionRef(Ref versionRef) {
      this.versionRef = versionRef;
    }

    @Override
    public Map withVersionRef(Ref versionRef) {
      setVersionRef(versionRef);
      return this;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Space<T extends Space> extends Identifiable<T> implements FilteringSource<T>, VersionedSource<T> {
    @JsonView({Public.class, Static.class})
    private Filters filters;
    @JsonView({Public.class, Static.class})
    private Ref versionRef = new Ref(Ref.HEAD);

    @Override
    public Filters getFilters() {
      return filters;
    }

    @Override
    public void setFilters(Filters filters) {
      this.filters = filters;
      if(filters != null && filters.getSpatialFilter() != null && filters.getSpatialFilter().getGeometry() != null
        && GeometryValidator.isWorldBoundingBox(filters.getSpatialFilter().getGeometry())){
        //Remove spatialFilter if it is a world bounding box
        this.filters = null;
      }
    }

    @Override
    public T withFilters(Filters filters) {
      setFilters(filters);
      return (T) this;
    }

    @Override
    public Ref getVersionRef() {
      return versionRef;
    }

    @Override
    public void setVersionRef(Ref versionRef) {
      this.versionRef = versionRef;
    }

    @Override
    public T withVersionRef(Ref versionRef) {
      setVersionRef(versionRef);
      return (T) this;
    }

    @Override
    public Set<String> getResourceKeys() {
      Set<String> resourceKeys = new HashSet<>(super.getResourceKeys());

      String extendedSpaceId = loadExtendedId();
      if (extendedSpaceId != null)
        resourceKeys.add(extendedSpaceId);

      return resourceKeys;
    }

    public String loadExtendedId() {
      try {
        Extension extension = HubWebClient.getInstance(Config.instance.HUB_ENDPOINT).loadSpace(getId()).getExtension();
        return extension == null ? null : extension.getSpaceId();
      }
      catch (WebClientException e) {
        //Ignore if space is not present (anymore)
        if (!(e instanceof ErrorResponseException errorResponseException && errorResponseException.getStatusCode() == 404))
          throw new RuntimeException(e);
      }
      return null;
    }
  }
}
