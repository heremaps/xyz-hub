/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.xyz.httpconnector.util.jobs.validate;

import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.models.geojson.coordinates.WKTHelper;
import com.here.xyz.models.geojson.implementation.Geometry;

public class ExportValidator extends Validator{
    protected static int VML_EXPORT_MIN_TARGET_LEVEL = 4;
    protected static int VML_EXPORT_MAX_TARGET_LEVEL = 13;
    protected static int VML_EXPORT_MAX_TILES_PER_FILE = 8192;

    public static void setExportDefaults(Export job){
        setJobDefaults(job);

        if(job.getExportTarget() != null && job.getExportTarget().getType().equals(Export.ExportTarget.Type.VML)){
            
           if( job.getCsvFormat() != null && job.getCsvFormat().equals(Job.CSVFormat.PARTITIONID_FC_B64) ) return;

           job.setCsvFormat(Job.CSVFormat.TILEID_FC_B64);

           if(job.getMaxTilesPerFile() == 0)
            job.setMaxTilesPerFile(ExportValidator.VML_EXPORT_MAX_TILES_PER_FILE);
        }
    }

    public static void validateExportCreation(Export job) throws Exception{
        validateJobCreation(job);

        if(job.getExportTarget() == null)
            throw new Exception("Please specify exportTarget!");

        if(job.getExportTarget().getType().equals(Export.ExportTarget.Type.VML)){

            switch(job.getCsvFormat())
            { case TILEID_FC_B64 : 
              case PARTITIONID_FC_B64 : break;
              default: throw new Exception("Invalid Format! Allowed ["+ Job.CSVFormat.TILEID_FC_B64+ "," + Job.CSVFormat.PARTITIONID_FC_B64 +"]");
            }

            if(job.getExportTarget().getTargetId() == null)
                throw new Exception("Please specify the targetId!");

            if( !job.getCsvFormat().equals(Job.CSVFormat.PARTITIONID_FC_B64) )
            {   
             if( job.getTargetLevel() == null )
                 throw new Exception("Please specify targetLevel! Allowed range ["+ ExportValidator.VML_EXPORT_MIN_TARGET_LEVEL +":"+ ExportValidator.VML_EXPORT_MAX_TARGET_LEVEL +"]");

             if(job.getTargetLevel() < ExportValidator.VML_EXPORT_MIN_TARGET_LEVEL || job.getTargetLevel() > ExportValidator.VML_EXPORT_MAX_TARGET_LEVEL)
                throw new Exception("Invalid targetLevel! Allowed range ["+ ExportValidator.VML_EXPORT_MIN_TARGET_LEVEL +":"+ ExportValidator.VML_EXPORT_MAX_TARGET_LEVEL +"]");
            }

        }else if(job.getExportTarget().getType().equals(Export.ExportTarget.Type.DOWNLOAD)){

            switch(job.getCsvFormat())
            { case JSON_WKB : 
              case GEOJSON : break;
              default: throw new Exception("Invalid Format! Allowed ["+ Job.CSVFormat.JSON_WKB +","+ Job.CSVFormat.GEOJSON +"]");
            }
                
        }

        Export.Filters filters = job.getFilters();
        if(filters != null){
            if(filters.getSpatialFilter() != null){
                Geometry geometry = filters.getSpatialFilter().getGeometry();
                if(geometry == null)
                    throw new Exception("Please specify a geometry for the spatial filter!");
                else{
                    try {
                        geometry.validate();
                        WKTHelper.geometryToWKB(geometry);
                    }catch (Exception e){
                        throw new Exception("Cant parse filter geometry!");
                    }
                }
            }
        }
    }
}
