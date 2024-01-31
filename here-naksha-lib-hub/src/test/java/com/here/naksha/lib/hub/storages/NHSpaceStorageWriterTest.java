package com.here.naksha.lib.hub.storages;


import static com.here.naksha.lib.common.assertions.WriteXyzCollectionsAssertions.assertThatWriteXyzCollections;
import static com.here.naksha.lib.common.assertions.WriteXyzFeaturesAssertions.assertThatWriteXyzFeatures;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.here.naksha.lib.common.TestNakshaContext;
import com.here.naksha.lib.core.EventPipeline;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaAdminCollection;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.naksha.Space;
import com.here.naksha.lib.core.models.storage.EWriteOp;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.SuccessResult;
import com.here.naksha.lib.core.models.storage.WriteRequest;
import com.here.naksha.lib.core.models.storage.WriteXyzCollections;
import com.here.naksha.lib.core.models.storage.WriteXyzFeatures;
import com.here.naksha.lib.hub.EventPipelineFactory;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class NHSpaceStorageWriterTest {

  private static final String CUSTOM_SPACE = "customSpace";

  @Mock
  INaksha naksha;

  @Mock
  EventPipelineFactory eventPipelineFactory;

  private NHSpaceStorageWriter writer;
  private NakshaContext nakshaContext;

  @BeforeEach
  void setup() {
    MockitoAnnotations.openMocks(this);
    nakshaContext = TestNakshaContext.newTestNakshaContext();
    writer = new NHSpaceStorageWriter(
        naksha,
        Map.of(
            NakshaAdminCollection.SPACES, List.of(mock(IEventHandler.class)),
            CUSTOM_SPACE, List.of(mock(IEventHandler.class))
        ),
        eventPipelineFactory,
        nakshaContext,
        false
    );
  }

  @Test
  void shouldInvokeTwoPipelinesOnDeleteSpaceRequest() {
    // Given: Configured event pipeline spy - used for verifying that space entry deletion was invoked
    EventPipeline eventPipeline = alwaysSucceedingPipeline();

    // And: delete space request
    WriteXyzFeatures deleteSpaceRequest = new WriteXyzFeatures(NakshaAdminCollection.SPACES).delete(CUSTOM_SPACE, null);

    // When: executing delete space request
    Result result = writer.execute(deleteSpaceRequest);

    // Then: Event Pipeline received Purge Collection request
    assertThatWriteXyzCollections(requestPassedToPipeline(eventPipeline, WriteXyzCollections.class))
        .hasSingleCodecThat(collectionCodec -> collectionCodec
            .hasWriteOp(EWriteOp.PURGE)
            .hasCollectionWithId(CUSTOM_SPACE)
        );

    // And: Event Pipeline received Delete Space Entry request
    assertThatWriteXyzFeatures(requestPassedToPipeline(eventPipeline, WriteXyzFeatures.class))
        .hasSingleCodecThat(featureCodec -> featureCodec
            .hasWriteOp(EWriteOp.DELETE)
            .hasId(CUSTOM_SPACE)
        );

    // And: Result of the whole operation is positive
    assertInstanceOf(SuccessResult.class, result);
  }

  @Test
  void shouldNotTriggerSpaceEntryDeletionWhenPurgingFailed() {
    // Given: Configured event pipeline spy that fails on WriteCollections
    EventPipeline eventPipeline = eventPipelineFailingOn(WriteXyzCollections.class);

    // And: delete space request
    WriteXyzFeatures deleteSpaceRequest = new WriteXyzFeatures(NakshaAdminCollection.SPACES).delete(CUSTOM_SPACE, null);

    // When: executing delete space request
    Result result = writer.execute(deleteSpaceRequest);

    // Then: Event Pipeline received Purge Collection request
    assertThatWriteXyzCollections(requestPassedToPipeline(eventPipeline, WriteXyzCollections.class))
        .hasSingleCodecThat(collectionCodec -> collectionCodec
            .hasWriteOp(EWriteOp.PURGE)
            .hasCollectionWithId(CUSTOM_SPACE)
        );

    // And: Space entry in admin collection does not get deleted (purging failed) - Event Pipeline does not receive Delete Space Entry request
    verify(eventPipeline, never()).sendEvent(any(WriteXyzFeatures.class));

    // And: Result of the whole operation is negative
    assertInstanceOf(ErrorResult.class, result);
  }

  @Test
  void shouldFailWhenSpaceEntryDeletionFailed() {
    // Given: Configured event pipeline spy that fails on WriteFeatures
    EventPipeline eventPipeline = eventPipelineFailingOn(WriteXyzFeatures.class);

    // And: delete space request
    WriteXyzFeatures deleteSpaceRequest = new WriteXyzFeatures(NakshaAdminCollection.SPACES).delete(CUSTOM_SPACE, null);

    // When: executing delete space request
    Result result = writer.execute(deleteSpaceRequest);

    // Then: Event Pipeline received Purge Collection request
    assertThatWriteXyzCollections(requestPassedToPipeline(eventPipeline, WriteXyzCollections.class))
        .hasSingleCodecThat(collectionCodec -> collectionCodec
            .hasWriteOp(EWriteOp.PURGE)
            .hasCollectionWithId(CUSTOM_SPACE)
        );

    // And: Event Pipeline received Delete Space Entry request
    assertThatWriteXyzFeatures(requestPassedToPipeline(eventPipeline, WriteXyzFeatures.class))
        .hasSingleCodecThat(featureCodec -> featureCodec
            .hasWriteOp(EWriteOp.DELETE)
            .hasId(CUSTOM_SPACE)
        );

    // And: Result of the whole operation is negative (space entry deletion failed)
    assertInstanceOf(ErrorResult.class, result);
  }

  private <T extends WriteRequest<?, ?, ?>> T requestPassedToPipeline(EventPipeline eventPipeline, Class<T> reqType) {
    ArgumentCaptor<T> requestPassedToPipeline = ArgumentCaptor.forClass(reqType);
    verify(eventPipeline).sendEvent(requestPassedToPipeline.capture());
    return requestPassedToPipeline.getValue();
  }

  private EventPipeline alwaysSucceedingPipeline() {
    return eventPipelineFailingOn(null);
  }

  private <T extends WriteRequest<?, ?, ?>> EventPipeline eventPipelineFailingOn(@Nullable Class<T> reqType) {
    EventPipeline eventPipeline = spy(new EventPipeline(naksha));
    when(eventPipeline.sendEvent(any())).thenReturn(new SuccessResult());
    if (reqType != null) {
      when(eventPipeline.sendEvent(any(reqType))).thenReturn(new ErrorResult(XyzError.ILLEGAL_ARGUMENT, "Configured to fail"));
    }
    clearInvocations(eventPipeline);
    when(eventPipelineFactory.eventPipeline()).thenReturn(eventPipeline);
    return eventPipeline;
  }
}