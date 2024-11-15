package com.here.xyz.jobs.util.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import java.util.Random;

public class ContentCreator {
    /** Generate content */
    public static byte[] generateImportFileContent(ImportFilesToSpace.Format format, int featureCnt) {
        String output = "";

        for (int i = 1; i <= featureCnt; i++) {
            output += generateContentLine(format, i);
        }
        return output.getBytes();
    }

    public static String generateContentLine(ImportFilesToSpace.Format format, int i){
        Random rd = new Random();
        String lineSeparator = "\n";

        if(format.equals(ImportFilesToSpace.Format.CSV_JSON_WKB))
            return "\"{'\"properties'\": {'\"test'\": "+i+"}}\",01010000A0E61000007DAD4B8DD0AF07C0BD19355F25B74A400000000000000000"+lineSeparator;
        else if(format.equals(ImportFilesToSpace.Format.CSV_GEOJSON))
            return "\"{'\"type'\":'\"Feature'\",'\"geometry'\":{'\"type'\":'\"Point'\",'\"coordinates'\":["+(rd.nextInt(179))+"."+(rd.nextInt(100))+","+(rd.nextInt(79))+"."+(rd.nextInt(100))+"]},'\"properties'\":{'\"test'\":"+i+"}}\""+lineSeparator;
        else
            return "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":["+(rd.nextInt(179))+"."+(rd.nextInt(100))+","+(rd.nextInt(79))+"."+(rd.nextInt(100))+"]},\"properties\":{\"test\":"+i+"}}"+lineSeparator;
    }

    public static FeatureCollection generateRandomFeatureCollection(int featureCnt) {
        FeatureCollection fc = new FeatureCollection();
        try {
            for (int i = 0; i < featureCnt; i++)
                fc.getFeatures().add(new Feature().withProperties(new Properties().with("test", i))
                        .withGeometry(new Point().withCoordinates(new PointCoordinates(i, i % 90))));
        }catch (JsonProcessingException e){}

        return fc;
    }

    public static FeatureCollection generateRandomFeatureCollection(int featureCnt,float xmin, float ymin, float xmax, float ymax) {
        FeatureCollection fc = new FeatureCollection();
        Random random = new Random();

        try {
            for (int i = 0; i < featureCnt; i++)
                fc.getFeatures().add(new Feature().withProperties(new Properties().with("test", i).with("all","common"))
                        .withGeometry(new Point().withCoordinates(new PointCoordinates( (xmin + random.nextFloat() * (xmax - xmin)), 
                                                                                        (ymin + random.nextFloat() * (ymax - ymin))))));
        }catch (JsonProcessingException e){}

        return fc;
    }


    public static Feature getFeatureFromCSVLine(String csvLine) throws JsonProcessingException {
        return XyzSerializable.deserialize( csvLine.substring(1, csvLine.lastIndexOf(",") -1 ).replaceAll("'\"","\""), Feature.class);
    }

    public static Geometry getWKBFromCsvLine(String csvLine) throws ParseException {
        String geomAsWKB = csvLine.substring(csvLine.lastIndexOf(",") + 1 );
        byte[] aux = WKBReader.hexToBytes(geomAsWKB);
        /** Try to read WKB */
        org.locationtech.jts.geom.Geometry read = new WKBReader().read(aux);
        return Geometry.convertJTSGeometry(read);
    }
}
