package com.here.xyz.jobs.util.test;

import com.fasterxml.jackson.core.JsonProcessingException;

import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;

import java.util.Random;

public class ContentCreator {
    /** Generate content */
    public static byte[] generateImportFileContent(int featureCnt) {
        String output = "";

        for (int i = 1; i <= featureCnt; i++) {
            output += generateContentLine(i);
        }
        return output.getBytes();
    }

    public static String generateContentLine(int i){
        Random rd = new Random();
        String lineSeparator = "\n";

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
}
