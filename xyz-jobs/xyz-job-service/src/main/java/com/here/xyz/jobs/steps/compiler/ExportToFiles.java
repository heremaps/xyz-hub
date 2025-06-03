/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz.jobs.steps.compiler;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;

import com.here.xyz.util.geo.GeoTools;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.datasets.DatasetDescription.Space;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.datasets.files.GeoJson;
import com.here.xyz.jobs.datasets.filters.SpatialFilter;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.JobCompiler.CompilationError;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.responses.StatisticsResponse;
import java.util.Map;

import javax.xml.crypto.dsig.TransformException;
import org.geotools.api.referencing.FactoryException;
import org.locationtech.jts.geom.Geometry;

public class ExportToFiles implements JobCompilationInterceptor {
  @Override
  public boolean chooseMe(Job job) {
    return job.getProcess() == null && Space.class.equals(job.getSource().getClass()) && job.getTarget() instanceof Files targetFiles
        && targetFiles.getOutputSettings().getFormat() instanceof GeoJson;
  }

  @Override
  public CompilationStepGraph compile(Job job) {
    return (CompilationStepGraph) new CompilationStepGraph()
        .addExecution(compile(job.getId(), (Space) job.getSource()));
  }

  public static ExportSpaceToFiles compile(String jobId, Space source) {
    return new ExportSpaceToFiles()
        .withProvidedVersionRef(source.getVersionRef())
        .withSpaceId(source.getId())
        .withJobId(jobId)
        .withSpatialFilter(source.getFilters() != null ? source.getFilters().getSpatialFilter() : null)
        .withPropertyFilter(source.getFilters() != null ? source.getFilters().getPropertyFilter() : null)
        .withContext(source.getFilters() != null ? source.getFilters().getContext() : null)
        .withVersionRef(source.getVersionRef())
        .withOutputMetadata(Map.of(source.getClass().getSimpleName().toLowerCase(), source.getId()));
  }

  private static HubWebClient hubWebClient() {
    return HubWebClient.getInstance(Config.instance.HUB_ENDPOINT);
  }

  public static boolean canExportFromDb(Space source) throws ValidationException {

   String spaceId = source.getId(); 
   SpaceContext sourceContext;
   StatisticsResponse sourceStatistics;

   try {
     sourceContext = hubWebClient().loadSpace(spaceId).getExtension() != null ? EXTENSION : DEFAULT;
     sourceStatistics = hubWebClient().loadSpaceStatistics(spaceId, sourceContext); 
   } catch (WebClientException e) {
      throw new CompilationError("Error resolving resource information: " + e.getMessage(), e);
   }

   boolean hasFilters = source.getFilters() != null 
                        && (   source.getFilters().getPropertyFilter() != null
                            || source.getFilters().getSpatialFilter() != null );

   long maxAllowedFeatureCount = 100_000l;

   if(!hasFilters)  // less then 100k features ok to export from DB
     return sourceStatistics.getCount().getValue() <= maxAllowedFeatureCount;

   SpatialFilter spatialFilter = source.getFilters().getSpatialFilter();

   if( spatialFilter != null && spatialFilter != null )
   {
    try { spatialFilter.validateSpatialFilter(); } 
    catch (ValidationException e) 
    { throw e; } 
    
    Geometry jtsGeometry = spatialFilter.getGeometry().getJTSGeometry();
    
    if(jtsGeometry != null && !jtsGeometry.isValid())
     throw new ValidationException("Invalid geometry in spatialFilter!");

    try {
      long MAX_ALLOWED_SPATALFILTER_AREA_IN_SQUARE_KM = 1000l; //tbd: 1000km2
      Geometry bufferedGeo = GeoTools.applyBufferInMetersToGeometry(jtsGeometry, spatialFilter.getRadius());
      int areaInSquareKilometersFromGeometry = (int) GeoTools.getAreaInSquareKilometersFromGeometry(bufferedGeo);

      if (areaInSquareKilometersFromGeometry <= MAX_ALLOWED_SPATALFILTER_AREA_IN_SQUARE_KM) 
       return true;
    } catch (FactoryException | TransformException | org.geotools.api.referencing.operation.TransformException e) {
      throw new ValidationException("Error calculating area of spatialFilter: " + e.getMessage(), e);
    }
   }
   
//TODO: check if propertyFilter is not null and if so "calculate" the size of the result set and decide if it is ok to export from DB
//   PropertiesQuery propertyFilter = source.getFilters().getPropertyFilter();

   return false;
  }

}
