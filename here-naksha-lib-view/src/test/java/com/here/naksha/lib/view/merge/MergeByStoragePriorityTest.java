package com.here.naksha.lib.view.merge;

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodec;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodecFactory;
import com.here.naksha.lib.view.ViewLayerRow;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MergeByStoragePriorityTest {

  XyzFeatureCodecFactory factory = XyzFeatureCodecFactory.get();
  MergeByStoragePriority<XyzFeature, XyzFeatureCodec> mergeStrategy = new MergeByStoragePriority<>();

  @Test
  void checkPriorityMerge() {
    // given
    List<ViewLayerRow<XyzFeature, XyzFeatureCodec>> singleRowFeatures = new ArrayList<>();

    XyzFeatureCodec f1 = factory.newInstance();
    XyzFeatureCodec f2 = factory.newInstance();
    XyzFeatureCodec f3 = factory.newInstance();

    singleRowFeatures.add(new ViewLayerRow<>(f1, 1, null));
    singleRowFeatures.add(new ViewLayerRow<>(f2, 0, null));
    singleRowFeatures.add(new ViewLayerRow<>(f3, 2, null));

    // when
    XyzFeatureCodec outputFeature = mergeStrategy.apply(singleRowFeatures);

    // then
    assertSame(f2,  outputFeature);
  }

  @Test
  void checkSamePriorityMerge() {
    // given
    List<ViewLayerRow<XyzFeature, XyzFeatureCodec>> singleRowFeatures = new ArrayList<>();

    XyzFeatureCodec f1 = factory.newInstance();
    XyzFeatureCodec f2 = factory.newInstance();
    XyzFeatureCodec f3 = factory.newInstance();

    singleRowFeatures.add(new ViewLayerRow<>(f1, 0, null));
    singleRowFeatures.add(new ViewLayerRow<>(f2, 0, null));
    singleRowFeatures.add(new ViewLayerRow<>(f3, 2, null));

    // when
    XyzFeatureCodec outputFeature = mergeStrategy.apply(singleRowFeatures);

    // then should pick first on list
    assertSame(f1,  outputFeature);
  }

  @Test
  void checkEmptyMerge() {
    // given
    List<ViewLayerRow<XyzFeature, XyzFeatureCodec>> singleRowFeatures = new ArrayList<>();

    // expect
    assertThrows(NoSuchElementException.class, () -> mergeStrategy.apply(singleRowFeatures));
  }

  @Test
  void checkNull() {
    // expect
    assertThrows(NullPointerException.class, () -> mergeStrategy.apply(null));
  }
}
