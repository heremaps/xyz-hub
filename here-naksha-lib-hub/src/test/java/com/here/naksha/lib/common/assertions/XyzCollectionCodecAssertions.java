package com.here.naksha.lib.common.assertions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.naksha.lib.core.models.naksha.XyzCollection;
import com.here.naksha.lib.core.models.storage.EWriteOp;
import com.here.naksha.lib.core.models.storage.XyzCollectionCodec;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;

public class XyzCollectionCodecAssertions {
  private final XyzCollectionCodec subject;

  private XyzCollectionCodecAssertions(XyzCollectionCodec subject){
    this.subject = subject;
  }

  public static XyzCollectionCodecAssertions assertThatXyzCollection(XyzCollectionCodec collectionCodec){
    return new XyzCollectionCodecAssertions(collectionCodec);
  }

  public XyzCollectionCodecAssertions hasWriteOp(EWriteOp expectedOp){
    assertEquals(expectedOp, EWriteOp.get(subject.getOp()));
    return this;
  }

  public XyzCollectionCodecAssertions hasCollectionWithId(String expectedId){
    assertEquals(expectedId, getRequiredCollection().getId());
    return this;
  }

  private @NotNull XyzCollection getRequiredCollection(){
    XyzCollection collection = subject.getFeature();
    Assertions.assertNotNull(collection, "Codec's collection can't be null");
    return collection;
  }
}
