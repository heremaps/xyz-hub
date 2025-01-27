package com.here.naksha.storage.http.connector.integration.tests;

import com.here.naksha.storage.http.connector.integration.utils.DataHub;
import com.here.naksha.storage.http.connector.integration.utils.Naksha;
import io.restassured.response.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.here.naksha.storage.http.connector.integration.utils.Commons.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PropSearchTest {

    public static final String BBOX_PATH_AND_PARAMS = "bbox?west=-10&north=10&east=10&south=-10&";

    @BeforeEach
    void setUp() {
        rmAllFeatures();
    }

    @Test
    void singleOperations() {
        DataHub.createFeatureFromJsonTemplateFile("propsearch/feature_template.json", "1", """
                "prop1" : 1"""
        );
        DataHub.createFeatureFromJsonTemplateFile("propsearch/feature_template.json", "2", """
                "prop1" : 2"""
        );
        DataHub.createFeatureFromJsonTemplateFile("propsearch/feature_template.json", "3", """
                "prop1" : 3"""
        );

        assertPropSearchHasShortIds("p.prop1=1", List.of("1"));
        assertPropSearchHasShortIds("p.prop1=2", List.of("2"));
        assertPropSearchHasShortIds("p.prop1!=1", List.of("2","3"));

        assertPropSearchHasShortIds("p.prop1=gt=2", List.of("3"));
        assertPropSearchHasShortIds("p.prop1=gte=2", List.of("2", "3"));

        assertPropSearchHasShortIds("p.prop1=lt=2", List.of("1"));
        assertPropSearchHasShortIds("p.prop1=lte=2", List.of("1", "2"));
    }

    @Test
    void combinedOperations() {
        DataHub.createFeatureFromJsonTemplateFile("propsearch/feature_template.json", "A1", """
                "prop1" : "A", "prop2" : 1"""
        );
        DataHub.createFeatureFromJsonTemplateFile("propsearch/feature_template.json", "A2", """
                "prop1" : "A", "prop2" : 2"""
        );
        DataHub.createFeatureFromJsonTemplateFile("propsearch/feature_template.json", "B1", """
                "prop1" : "B", "prop2" : 1"""
        );
        DataHub.createFeatureFromJsonTemplateFile("propsearch/feature_template.json", "B2", """
                "prop1" : "B", "prop2" : 2"""
        );
        DataHub.createFeatureFromJsonTemplateFile("propsearch/feature_template.json", "C1", """
                "prop1" : "C", "prop2" : 1"""
        );
        DataHub.createFeatureFromJsonTemplateFile("propsearch/feature_template.json", "C2", """
                "prop1" : "C", "prop2" : 2"""
        );

        assertPropSearchHasShortIds("p.prop1=A&p.prop2=1", List.of("A1")); // and
        assertPropSearchHasShortIds("p.prop1=A,B", List.of("A1","A2","B1","B2")); // or
        assertPropSearchHasShortIds("p.prop1=A,B&p.prop2=1", List.of("A1","B1")); // or, and
        assertPropSearchHasShortIds("p.prop1!=A,B&p.prop2=1", List.of("A1","B1", "C1")); // or != (always true), and
        assertPropSearchHasShortIds("p.prop1!=A&p.prop1!=B&p.prop2=1", List.of("C1")); // and !=
        assertPropSearchHasShortIds("p.prop2=gt=1&p.prop2=lte=3", List.of("A2","B2","C2"));

    }

    @Test
    void notSupportedOperations(){
        DataHub.createFeatureFromJsonTemplateFile("propsearch/feature_template.json", "1", """
                "prop1" : 1"""
        );

        String params = BBOX_PATH_AND_PARAMS + "p.prop1=cs=1";
        Response nakshaResponse = Naksha.request().urlEncodingEnabled(false).get(params);
        assertEquals(nakshaResponse.jsonPath().getString("type"),"ErrorResponse");
        assertEquals(nakshaResponse.jsonPath().getString("error"),"Exception");
        System.out.println();
    }

    void assertPropSearchHasShortIds(String propsearch, List<String> shortIds) {
        String params = BBOX_PATH_AND_PARAMS + propsearch;
        Response nakshaResponse = Naksha.request().urlEncodingEnabled(false).get(params);
        Response dataHubResponse = DataHub.request().urlEncodingEnabled(false).get(params);
        assertTrue(responseHasExactShortIds(shortIds, nakshaResponse));
        assertTrue(responseHasExactShortIds(shortIds, dataHubResponse));
    }


}
