package com.here.naksha.lib.handlers;

import static com.here.naksha.lib.handlers.AbstractEventHandler.EventProcessingStrategy.PROCESS;
import static com.here.naksha.lib.handlers.AbstractEventHandler.EventProcessingStrategy.SEND_UPSTREAM_WITHOUT_PROCESSING;
import static com.here.naksha.lib.handlers.AbstractEventHandler.EventProcessingStrategy.SUCCEED_WITHOUT_PROCESSING;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.calls;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.here.naksha.lib.core.IEvent;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.SuccessResult;
import com.here.naksha.lib.core.util.StreamInfo;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class AbstractEventHandlerTest {

  private AbstractEventHandler eventHandler;

  @BeforeEach
  void setup() {
    eventHandler = mock(AbstractEventHandler.class, CALLS_REAL_METHODS);
  }

  @Test
  void shouldProcessEvent() {
    // Given: Event that should be processed
    IEvent event = mock(IEvent.class);
    when(eventHandler.processingStrategyFor(event)).thenReturn(PROCESS);

    // When: Processing such event
    eventHandler.processEvent(event);

    // Then: Handler actually processes it
    verify(eventHandler).process(event);
  }

  @Test
  void shouldReturnSuccessImmediately() {
    // Given: Event that should not be processed and that should immediately succeed
    IEvent event = mock(IEvent.class);
    when(eventHandler.processingStrategyFor(event)).thenReturn(SUCCEED_WITHOUT_PROCESSING);

    // When: Processing such event
    Result result = eventHandler.processEvent(event);

    // Then: Event handler did not process it
    verify(eventHandler, never()).process(event);

    // And: Event was not sent to upstream
    verify(event, never()).sendUpstream();

    // And: The result of operation was successful
    assertInstanceOf(SuccessResult.class, result);
  }

  @Test
  void shouldSendEventToUpstreamImmediately() {
    // Given: Event that should not be processed and that should immediately succeed
    IEvent event = mock(IEvent.class);
    when(eventHandler.processingStrategyFor(event)).thenReturn(SEND_UPSTREAM_WITHOUT_PROCESSING);

    // When: Processing such event
    eventHandler.processEvent(event);

    // Then: Event handler did not process it
    verify(eventHandler, never()).process(event);

    // And: Event was sent to upstream
    verify(event).sendUpstream();
  }

  @Test
  void shouldAddStorageIdFromStreamInfo(){
    // Given: available Stream Info
    StreamInfo streamInfo = spy(new StreamInfo());

    // And: context with configured Stream Info
    NakshaContext nakshaContext = new NakshaContext();
    nakshaContext.attachStreamInfo(streamInfo);

    // And: StorageId to apply
    String storageId = "some_storage";

    // When: adding storage id to stream via event handler
    eventHandler.addStorageIdToStreamInfo(storageId, nakshaContext);

    // Then: StreamInfo was enriched with StorageId
    verify(streamInfo).setStorageIdIfMissing(storageId);
  }
}