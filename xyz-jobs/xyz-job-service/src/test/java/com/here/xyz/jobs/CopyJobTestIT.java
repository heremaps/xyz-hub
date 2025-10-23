package com.here.xyz.jobs;

import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.FileOutputSettings;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.datasets.files.GeoJson;
import com.here.xyz.jobs.steps.JobCompiler.CompilationError;
import com.here.xyz.jobs.util.test.JobTestBase;
import com.here.xyz.models.geojson.coordinates.LinearRingCoordinates;
import com.here.xyz.models.geojson.coordinates.PolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.Position;
import com.here.xyz.models.geojson.implementation.Polygon;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Ref.InvalidRef;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Space.ConnectorRef;
import com.here.xyz.models.hub.Tag;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static com.here.xyz.jobs.datasets.files.FileFormat.EntityPerLine.Feature;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class CopyJobTestIT extends JobTest {

  static private String SrcSpc    = "testCopy-Source-09", 
                        TrgSpc    = "testCopy-Target-09",
                        EndTagId  = "tagV2",
                        versionRange = "1.." + EndTagId,
                        versionRangeInvalid = EndTagId +".." + EndTagId;


  static private long EndVersion = 2;
                        //OtherCntr = "psql_db2_hashed",
                        //TrgRmtSpc = "testCopy-Target-09-remote",
                        //propertyFilter = "p.all=common";
         
  static private Polygon spatialSearchGeom;
  static private float xmin = 7.0f, ymin = 50.0f, xmax = 7.1f, ymax = 50.1f;
  static {
   
   LinearRingCoordinates lrc = new LinearRingCoordinates();
   lrc.add(new Position(xmin, ymin));
   lrc.add(new Position(xmax, ymin));
   lrc.add(new Position(xmax, ymax));
   lrc.add(new Position(xmin, ymax));
   lrc.add(new Position(xmin, ymin));
   PolygonCoordinates pc = new PolygonCoordinates();
   pc.add(lrc);
   spatialSearchGeom = new Polygon().withCoordinates( pc );

  }

  @BeforeEach
  public void setup() throws SQLException {
      
      cleanup();

      createSpace(new Space().withId(SrcSpc).withVersionsToKeep(100),false);
      createSpace(new Space().withId(TrgSpc).withVersionsToKeep(100),false);
      ////createSpace(new Space().withId(TrgRmtSpc).withVersionsToKeep(100).withStorage(new ConnectorRef().withId(OtherCntr)),false);

      //write features source
      putRandomFeatureCollectionToSpace(SrcSpc, 20,xmin,ymin,xmax,ymax); // v1
      putRandomFeatureCollectionToSpace(SrcSpc, 10,xmin,ymin,xmax,ymax); // v2
      putRandomFeatureCollectionToSpace(SrcSpc, 10,xmin,ymin,xmax,ymax); // v3
      createTag(SrcSpc, new Tag().withId(EndTagId).withVersion(EndVersion));
      //write features target - non-empty-space
      putRandomFeatureCollectionToSpace(TrgSpc, 2,xmin,ymin,xmax,ymax);

      ////putRandomFeatureCollectionToSpace(TrgRmtSpc, 2,xmin,ymin,xmax,ymax);

  }

  @AfterEach
  public void cleanup() throws SQLException {
    deleteSpace(SrcSpc);
    deleteSpace(TrgSpc);
    deleteTag(SrcSpc,EndTagId);
  }

  protected void checkSucceededJob(Job job) throws IOException, InterruptedException {
     RuntimeStatus status = getJobStatus(job.getId());
     Assertions.assertEquals(RuntimeInfo.State.SUCCEEDED, status.getState());
     Assertions.assertEquals(status.getOverallStepCount(), status.getSucceededSteps());
  }

  private Job buildCopyJob(String ref) {
    return new Job()
            .withId(JOB_ID)
            .withDescription("Copy Job Test")
            .withSource(new DatasetDescription.Space<>().withId(SrcSpc).withVersionRef(ref == null ? new Ref("HEAD") : new Ref(ref)))
            .withTarget(new DatasetDescription.Space<>().withId(TrgSpc));
  }

  private static Stream<Arguments> provideParameters() {
    return Stream.of(
      Arguments.of(null, 40),
      Arguments.of(versionRange, 10),
      Arguments.of(versionRangeInvalid, 0)
    );
  }

 @ParameterizedTest
 @MethodSource("provideParameters")
 public void testSimpleCopy( String versionRange, int expectedFeaturesCopied ) throws Exception {
        Job copyJob = buildCopyJob( versionRange );

        if(expectedFeaturesCopied == 0)
        { // CompilationError expected
          Assertions.assertThrowsExactly(RuntimeException.class, () -> createSelfRunningJob(copyJob));
          return;
        }
           
        createSelfRunningJob(copyJob);
        checkSucceededJob(copyJob);

       List<Map> l =  getJobOutputs(copyJob.getId());

       Assertions.assertEquals( 1, l.size() );

       Assertions.assertEquals( expectedFeaturesCopied, l.get(0).get("featureCount") );
 }

}
