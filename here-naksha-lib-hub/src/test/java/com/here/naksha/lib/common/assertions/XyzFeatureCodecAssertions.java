package com.here.naksha.lib.common.assertions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.storage.EWriteOp;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodec;

public class XyzFeatureCodecAssertions {

  private final XyzFeatureCodec subject;

  private XyzFeatureCodecAssertions(XyzFeatureCodec featureCodec) {
    this.subject = featureCodec;
  }

  public static XyzFeatureCodecAssertions assertThatXyzFeatureCodec(XyzFeatureCodec featureCodec) {
    return new XyzFeatureCodecAssertions(featureCodec);
  }

  public XyzFeatureCodecAssertions hasWriteOp(EWriteOp expectedOp) {
    assertEquals(expectedOp, EWriteOp.get(subject.getOp()));
    return this;
  }

  public XyzFeatureCodecAssertions hasId(String expectedId) {
    assertEquals(expectedId, subject.getId());
    return this;
  }
}
