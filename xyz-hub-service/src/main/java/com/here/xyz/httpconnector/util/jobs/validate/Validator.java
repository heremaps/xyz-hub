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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.models.geojson.implementation.Feature;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import java.io.UnsupportedEncodingException;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

public class Validator {

    private static final Logger logger = LogManager.getLogger();

    public static void validateCSVLine(String csvLine, Job.CSVFormat csvFormat) throws UnsupportedEncodingException {

        if(csvLine != null && csvLine.endsWith("\r\n"))
            csvLine = csvLine.substring(0,csvLine.length()-4);
        else if(csvLine != null && (csvLine.endsWith("\n") || csvLine.endsWith("\r")))
            csvLine = csvLine.substring(0,csvLine.length()-2);

        switch (csvFormat){
            case GEOJSON:
                 validateGEOJSON(csvLine);
                break;
            case JSON_WKB:
                validateJSON_WKB(csvLine);
                break;
            case JSON_WKT:
                validateJSON_WKT(csvLine);
        }
    }

    private static void validateGEOJSON(String csvLine) throws UnsupportedEncodingException {
        try {
            /** Try to serialize JSON */
            String geoJson = csvLine.substring(1,csvLine.length()).replaceAll("'\"","\"");
            XyzSerializable.deserialize(geoJson, Feature.class);
        } catch (JsonProcessingException e) {
            logger.info("Bad Encoding: ",e);
            throw new UnsupportedEncodingException();
        } catch (Exception e) {
            logger.info("Bad Encoding: ",e);
            throw new UnsupportedEncodingException();
        }
    }

    private static void validateJSON_WKB(String csvLine) throws UnsupportedEncodingException {
        if(csvLine.lastIndexOf(",") != -1) {
            try {
                String json = csvLine.substring(1,csvLine.lastIndexOf(",")-1).replaceAll("'\"","\"");
                String wkb = csvLine.substring(csvLine.lastIndexOf(",")+1);

                byte[] aux = WKBReader.hexToBytes(wkb);
                /** Try to read WKB */
                new WKBReader().read(aux);
                /** Try to serialize JSON */
                new JSONObject(json);
            } catch (ParseException e) {
                logger.info("Bad WKB Encoding: ",e);
                throw new UnsupportedEncodingException();
            } catch (JSONException e) {
                logger.info("Bad JSON Encoding: ",e);
                throw new UnsupportedEncodingException();
            } catch (Exception e) {
                logger.info("Bad Encoding: ",e);
                throw new UnsupportedEncodingException();
            }
        }
    }

    private static void validateJSON_WKT(String csvLine) throws UnsupportedEncodingException {
        throw new NotImplementedException();
    }
}
