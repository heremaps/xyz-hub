package com.here.naksha.lib.common.assertions;

import static com.here.naksha.lib.common.assertions.XyzCollectionCodecAssertions.assertThatXyzCollection;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.naksha.lib.core.models.storage.WriteXyzCollections;
import com.here.naksha.lib.core.models.storage.XyzCollectionCodec;
import java.util.function.Consumer;
import org.junit.jupiter.api.Assertions;

public class WriteXyzCollectionsAssertions {

  private final WriteXyzCollections subject;

  private WriteXyzCollectionsAssertions(WriteXyzCollections subject) {
    this.subject = subject;
  }

  public static WriteXyzCollectionsAssertions assertThatWriteXyzCollections(WriteXyzCollections collections) {
    Assertions.assertNotNull(collections, "WriteXyzCollections subject can't be null");
    return new WriteXyzCollectionsAssertions(collections);
  }

  public WriteXyzCollectionsAssertions hasSingleCodecThat(Consumer<XyzCollectionCodecAssertions> collectionAssertions){
    return hasCodecsThat(collectionAssertions);
  }

  @SafeVarargs
  public final WriteXyzCollectionsAssertions hasCodecsThat(Consumer<XyzCollectionCodecAssertions>... collectionsAssertions) {
    assertEquals(collectionsAssertions.length, subject.features.size(), "Assertions and collections count don't match");
    for (int i = 0; i < collectionsAssertions.length; i++) {
      XyzCollectionCodec collectionSubject = subject.features.get(i);
      collectionsAssertions[i].accept(assertThatXyzCollection(collectionSubject));
    }
    return this;
  }
}
