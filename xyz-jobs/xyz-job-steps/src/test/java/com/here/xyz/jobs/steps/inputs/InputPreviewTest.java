package com.here.xyz.jobs.steps.inputs;

import com.here.xyz.jobs.steps.GroupPayloads;
import com.here.xyz.jobs.steps.JobPayloads;
import com.here.xyz.jobs.steps.SetPayloads;
import com.here.xyz.util.service.aws.s3.S3Uri;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class InputPreviewTest {

  private static final String JOB_ID = "job-preview-test";

  private Map<String, Map<String, Input.InputsMetadata>> originalMetadataCache;
  private Set<String> originalInputsCacheActive;

  @Before
  public void setup() throws Exception {
    Field metadataCacheField = Input.class.getDeclaredField("metadataCache");
    metadataCacheField.setAccessible(true);
    originalMetadataCache = (Map<String, Map<String, Input.InputsMetadata>>) metadataCacheField.get(null);

    Field inputsCacheActiveField = Input.class.getDeclaredField("inputsCacheActive");
    inputsCacheActiveField.setAccessible(true);
    originalInputsCacheActive = (Set<String>) inputsCacheActiveField.get(null);

    Map<String, Map<String, Map<String, Input.InputsMetadata>>> newMetadataCacheRoot = new WeakHashMap<>();

    Map<String, Map<String, Input.InputsMetadata>> groups = new HashMap<>();

    Map<String, Input.InputMetadata> set1Inputs = new HashMap<>();
    set1Inputs.put("s3://bucketA/key1", new Input.InputMetadata(100, false));
    set1Inputs.put("s3://bucketA/key2", new Input.InputMetadata(200, false));
    Input.InputsMetadata set1Meta = new Input.InputsMetadata(set1Inputs, new HashSet<>(Set.of(JOB_ID)), null, new S3Uri("bucketA", "prefixA"));

    Map<String, Input.InputsMetadata> groupA = new HashMap<>();
    groupA.put("set1", set1Meta);

    Map<String, Input.InputMetadata> set2Inputs = new HashMap<>();
    set2Inputs.put("s3://bucketB/k1", new Input.InputMetadata(50, false));
    Input.InputsMetadata set2Meta = new Input.InputsMetadata(set2Inputs, new HashSet<>(Set.of(JOB_ID)), null, new S3Uri("bucketB", "prefixB"));

    Map<String, Input.InputMetadata> set3Inputs = new HashMap<>();
    set3Inputs.put("s3://bucketB/k2", new Input.InputMetadata(10, false));
    set3Inputs.put("s3://bucketB/k3", new Input.InputMetadata(20, false));
    set3Inputs.put("s3://bucketB/k4", new Input.InputMetadata(30, false));
    Input.InputsMetadata set3Meta = new Input.InputsMetadata(set3Inputs, new HashSet<>(Set.of(JOB_ID)), null, new S3Uri("bucketB", "prefixB"));

    Map<String, Input.InputsMetadata> groupB = new HashMap<>();
    groupB.put("set2", set2Meta);
    groupB.put("set3", set3Meta);

    groups.put("groupA", groupA);
    groups.put("groupB", groupB);

    newMetadataCacheRoot.put(JOB_ID, groups);

    metadataCacheField.set(null, newMetadataCacheRoot);

    Set<String> newInputsCacheActive = new HashSet<>();
    newInputsCacheActive.add(JOB_ID);
    inputsCacheActiveField.set(null, newInputsCacheActive);
  }

  @After
  public void onClose() throws Exception {
    Field metadataCacheField = Input.class.getDeclaredField("metadataCache");
    metadataCacheField.setAccessible(true);
    metadataCacheField.set(null, originalMetadataCache);

    Field inputsCacheActiveField = Input.class.getDeclaredField("inputsCacheActive");
    inputsCacheActiveField.setAccessible(true);
    inputsCacheActiveField.set(null, originalInputsCacheActive);
  }

  @Test
  public void testPreviewInputGroupsSummariesAndSums() {
    GroupPayloads groupB = Input.previewInputGroups(JOB_ID, "groupB");

    Assertions.assertEquals(110L, groupB.getByteSize(), "Total bytes in groupB should be the sum of all set byte sizes.");
    Assertions.assertEquals(4L, groupB.getItemCount(), "Item count in groupB should be the total number of inputs across sets.");

    Map<String, SetPayloads> items = groupB.getSets();
    Assertions.assertEquals(2, items.size());
    Assertions.assertEquals(1L, items.get("set2").getItemCount());
    Assertions.assertEquals(50L, items.get("set2").getByteSize());
    Assertions.assertEquals(3L, items.get("set3").getItemCount());
    Assertions.assertEquals(60L, items.get("set3").getByteSize());
  }

  @Test
  public void testPreviewNonExistingGroupReturnsEmptySummary() {
    GroupPayloads none = Input.previewInputGroups(JOB_ID, "no-such-group");
    Assertions.assertNotNull(none);
    Assertions.assertEquals(0L, none.getByteSize());
    Assertions.assertEquals(0L, none.getItemCount());
    Assertions.assertTrue(none.getSets() == null || none.getSets().isEmpty());
  }

  @Test
  public void testPreviewInputsAggregatesAcrossGroups() {
    JobPayloads preview = Input.previewInputs(JOB_ID);

    Assertions.assertEquals(410L, preview.getByteSize());
    Assertions.assertEquals(6L, preview.getItemCount());

    Map<String, GroupPayloads> groups = preview.getGroups();
    Assertions.assertEquals(2, groups.size());

    GroupPayloads groupA = groups.get("groupA");
    Assertions.assertEquals(300L, groupA.getByteSize());
    Assertions.assertEquals(2L, groupA.getItemCount());

    GroupPayloads groupB = groups.get("groupB");
    Assertions.assertEquals(110L, groupB.getByteSize());
    Assertions.assertEquals(4L, groupB.getItemCount());
  }
}
