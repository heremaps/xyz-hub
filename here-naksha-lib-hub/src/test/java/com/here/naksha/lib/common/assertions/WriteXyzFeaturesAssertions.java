package com.here.naksha.lib.common.assertions;

import static com.here.naksha.lib.common.assertions.XyzFeatureCodecAssertions.assertThatXyzFeatureCodec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.here.naksha.lib.core.models.storage.WriteRequest;
import com.here.naksha.lib.core.models.storage.WriteXyzFeatures;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodec;
import java.util.function.Consumer;

public class WriteXyzFeaturesAssertions {
  private WriteXyzFeatures subject;

  private WriteXyzFeaturesAssertions(WriteXyzFeatures wf){
    this.subject = wf;
  }

  public static WriteXyzFeaturesAssertions assertThatWriteXyzFeatures(WriteXyzFeatures wf){
    return new WriteXyzFeaturesAssertions(wf);
  }

  public static WriteXyzFeaturesAssertions assertThatWriteXyzFeatures(WriteRequest<?, ?, ?> wf){
    assertInstanceOf(WriteXyzFeatures.class, wf);
    return new WriteXyzFeaturesAssertions((WriteXyzFeatures) wf);
  }

  public WriteXyzFeaturesAssertions hasSingleCodecThat(Consumer<XyzFeatureCodecAssertions> featureCodecAssertions){
    return hasCodecsThat(featureCodecAssertions);
  }

  @SafeVarargs
  public final WriteXyzFeaturesAssertions hasCodecsThat(Consumer<XyzFeatureCodecAssertions>... codecsAssertions){
    assertEquals(codecsAssertions.length, subject.features.size(), "Assertions and codecs count don't match");
    for(int i = 0; i < codecsAssertions.length; i++){
      XyzFeatureCodec codecSubject = subject.features.get(i);
      codecsAssertions[i].accept(assertThatXyzFeatureCodec(codecSubject));
    }
    return this;
  }
}
