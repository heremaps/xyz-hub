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
import com.here.xyz.hub.rest.ApiParam;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.models.geojson.coordinates.WKTHelper;
import com.here.xyz.models.geojson.implementation.Geometry;
import io.netty.handler.codec.http.HttpResponseStatus;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.PRECONDITION_FAILED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;

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

    public static void validateExportCreation(Export job) throws HttpException{
        validateJobCreation(job);

        if(job.getExportTarget() == null)
            throw new HttpException(BAD_REQUEST,("Please specify exportTarget!"));

        if(job.getExportTarget().getType().equals(Export.ExportTarget.Type.VML)){

            switch(job.getCsvFormat())
            { case TILEID_FC_B64 : 
              case PARTITIONID_FC_B64 : break;
              default: throw new HttpException(BAD_REQUEST,("Invalid Format! Allowed ["+ Job.CSVFormat.TILEID_FC_B64+ "," + Job.CSVFormat.PARTITIONID_FC_B64 +"]"));
            }

            if(job.getExportTarget().getTargetId() == null)
                throw new HttpException(BAD_REQUEST,("Please specify the targetId!"));

            if( !job.getCsvFormat().equals(Job.CSVFormat.PARTITIONID_FC_B64) )
            {   
             if( job.getTargetLevel() == null )
                 throw new HttpException(BAD_REQUEST,("Please specify targetLevel! Allowed range ["+ ExportValidator.VML_EXPORT_MIN_TARGET_LEVEL +":"+ ExportValidator.VML_EXPORT_MAX_TARGET_LEVEL +"]"));

             if(job.getTargetLevel() < ExportValidator.VML_EXPORT_MIN_TARGET_LEVEL || job.getTargetLevel() > ExportValidator.VML_EXPORT_MAX_TARGET_LEVEL)
                throw new HttpException(BAD_REQUEST,("Invalid targetLevel! Allowed range ["+ ExportValidator.VML_EXPORT_MIN_TARGET_LEVEL +":"+ ExportValidator.VML_EXPORT_MAX_TARGET_LEVEL +"]"));
            }

        }else if(job.getExportTarget().getType().equals(Export.ExportTarget.Type.DOWNLOAD)){

            switch(job.getCsvFormat())
            { case JSON_WKB : 
              case GEOJSON : break;
              default: throw new HttpException(BAD_REQUEST,("Invalid Format! Allowed ["+ Job.CSVFormat.JSON_WKB +","+ Job.CSVFormat.GEOJSON +"]"));
            }
                
        }

        Export.Filters filters = job.getFilters();
        if(filters != null){
            if(filters.getSpatialFilter() != null){
                Geometry geometry = filters.getSpatialFilter().getGeometry();
                if(geometry == null)
                    throw new HttpException(BAD_REQUEST,("Please specify a geometry for the spatial filter!"));
                else{
                    try {
                        geometry.validate();
                        WKTHelper.geometryToWKB(geometry);
                    }catch (Exception e){
                        throw new HttpException(BAD_REQUEST,("Cant parse filter geometry!"));
                    }
                }
            }
        }

        if(job.readParamPersistExport()){
            if(!job.getExportTarget().getType().equals(Export.ExportTarget.Type.VML))
                throw new HttpException(BAD_REQUEST,("Persistent Export not allowed for this target!"));
            if(filters != null)
                throw new HttpException(BAD_REQUEST,("Persistent Export is only allowed without filters!"));
        }
    }

    protected static void isValidForStart(Export job, ApiParam.Query.Incremental incremental) throws HttpException{
        if(!job.getStatus().equals(Job.Status.waiting))
            throw new HttpException(PRECONDITION_FAILED, "Invalid state: "+job.getStatus() +" execution is only allowed on status=waiting");

        if(!incremental.equals(ApiParam.Query.Incremental.DEACTIVATED)) {
            if(incremental.equals(ApiParam.Query.Incremental.CHANGES) && job.includesSecondLevelExtension()) {
                throw new HttpException(NOT_IMPLEMENTED, "Incremental Export of CHANGES is not supported for 2nd Level extended spaces!");
            }

            if(!job.getCsvFormat().equals(Job.CSVFormat.TILEID_FC_B64)){
                throw new HttpException(BAD_REQUEST, "CSV format is not supported!");
            }

            if (job.getExportTarget().getType().equals(Export.ExportTarget.Type.DOWNLOAD))
                throw new HttpException(HttpResponseStatus.BAD_REQUEST, "Incremental Export is available for Type Download!");

            if (job.getParams() == null || job.getParams().get("extends") == null)
                throw new HttpException(HttpResponseStatus.BAD_REQUEST, "Incremental Export is only possible on extended layers!");

            if(incremental.equals(ApiParam.Query.Incremental.FULL)) {
                /** We have to check if the super layer got exported in a persistent way */
                if(!job.isSuperSpacePersistent())
                    throw new HttpException(HttpResponseStatus.BAD_REQUEST, "Incremental Export requires persistent superLayer!");
            }
        }
    }
    
    public static void isValidForAbort(Job job) throws HttpException {
        /**
         * It is only allowed to abort a job inside executing state, because we have multiple nodes running.
         * During the execution we have running SQL-Statements - due to the abortion of them, the client which
         * has executed the Query will handle the abortion.
         * */
        if(!job.getStatus().equals(Job.Status.executing))
            throw new HttpException(PRECONDITION_FAILED, "Job is not in executing state - current status: "+job.getStatus());
    }
}
