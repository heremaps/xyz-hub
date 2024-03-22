package com.here.naksha.lib.handlers.internal;

import static com.here.naksha.lib.core.NakshaAdminCollection.EVENT_HANDLERS;
import static com.here.naksha.lib.core.NakshaAdminCollection.SPACES;
import static com.here.naksha.lib.core.models.XyzError.NOT_FOUND;
import static com.here.naksha.lib.core.models.storage.EExecutedOp.READ;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Named.named;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.here.naksha.lib.core.IEvent;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.naksha.Space;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.HeapCacheCursor;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.Request;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.SuccessResult;
import com.here.naksha.lib.core.models.storage.WriteXyzFeatures;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodec;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodecFactory;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.core.storage.IWriteSession;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class IntHandlerForSpacesTest {

  @Mock
  INaksha naksha;

  IntHandlerForSpaces handler;

  @BeforeEach
  void setup() {
    MockitoAnnotations.openMocks(this);
    handler = new IntHandlerForSpaces(naksha);
  }

  @Test
  void shouldAlwaysAllowDeletion() {
    // Given:
    IEvent event = eventWith(new WriteXyzFeatures(SPACES).delete(new XyzFeature("to_delete")));

    // And:
    writingToAdminSucceeds();

    // When
    Result result = handler.process(event);

    // Then
    assertInstanceOf(SuccessResult.class, result);
  }

  @ParameterizedTest
  @MethodSource("persistingWritesWithInvalidSpace")
  void shouldNotStoreSpaceThatViolatesBasicValidation(WriteXyzFeatures writeSpace) {
    // Given:
    IEvent event = eventWith(writeSpace);

    // And:
    writingToAdminSucceeds();

    // When
    Result result = handler.process(event);

    // Then
    assertInstanceOf(ErrorResult.class, result);
  }

  @ParameterizedTest
  @MethodSource("persistingSpaceWithoutValidHandlers")
  void shouldNotStoreSpaceWithMissingHandlers(WriteXyzFeatures writeSpace) {
    // Given:
    Space space = (Space) writeSpace.features.get(0).getFeature();
    IEvent event = eventWith(writeSpace);

    // And:
    List<String> existingHandlers = List.of("handler_1", "handler_2");
    List<String> missingHandlerIds = space.getEventHandlerIds().stream()
        .filter(id -> !existingHandlers.contains(id))
        .toList();

    // And
    handlersExist(existingHandlers);
    writingToAdminSucceeds();

    // When
    Result result = handler.process(event);

    // Then
    assertInstanceOf(ErrorResult.class, result);
    ErrorResult errorResult = (ErrorResult) result;
    assertEquals(NOT_FOUND, errorResult.reason);
    assertEquals(errorResult.message, "Following handlers defined for Space %s don't exist: %s".formatted(
        space.getId(),
        String.join(",", missingHandlerIds)
    ));
  }

  private static Stream<Named<WriteXyzFeatures>> persistingWritesWithInvalidSpace() {
    Space spaceWithoutTitle = space("no_title", null, "some_desc");
    Space spaceWithoutDescription = space("no_desc", "some_title", null);
    return Stream.of(
        named("PUT Space without title", new WriteXyzFeatures(SPACES).put(spaceWithoutTitle)),
        named("UPDATE Space without title", new WriteXyzFeatures(SPACES).update(spaceWithoutTitle)),
        named("CREATE Space without title", new WriteXyzFeatures(SPACES).create(spaceWithoutTitle)),
        named("PUT Space without description", new WriteXyzFeatures(SPACES).put(spaceWithoutDescription)),
        named("UPDATE Space without description", new WriteXyzFeatures(SPACES).update(spaceWithoutDescription)),
        named("CREATE Space without description", new WriteXyzFeatures(SPACES).create(spaceWithoutDescription))
    );
  }

  private static Stream<Named<WriteXyzFeatures>> persistingSpaceWithoutValidHandlers() {
    Space space = space("space_id", "no_desc", "some_title", List.of("handler_1", "handler_2", "handler_3"));
    return Stream.of(
        named("PUT Space without valid handlers", new WriteXyzFeatures(SPACES).put(space)),
        named("UPDATE Space without valid handlers", new WriteXyzFeatures(SPACES).update(space)),
        named("CREATE Space without valid handlers", new WriteXyzFeatures(SPACES).create(space))
    );
  }

  private static Space space(String id, String title, String desc) {
    return space(id, title, desc, emptyList());
  }

  private static Space space(String id, String title, String desc, List<String> handlersIds) {
    Space space = new Space(id);
    space.setTitle(title);
    space.setDescription(desc);
    for (String handlerId : handlersIds) {
      space.addHandler(handlerId);
    }
    return space;
  }

  private IEvent eventWith(Request request) {
    IEvent event = mock(IEvent.class);
    when(event.getRequest()).thenReturn(request);
    return event;
  }

  private void writingToAdminSucceeds() {
    IStorage admin = mock(IStorage.class);
    when(naksha.getAdminStorage()).thenReturn(admin);
    IWriteSession writeSession = mock(IWriteSession.class);
    when(writeSession.execute(any(WriteXyzFeatures.class))).thenReturn(new SuccessResult());
    when(admin.newWriteSession(any(NakshaContext.class), anyBoolean())).thenReturn(writeSession);
  }

  private void handlersExist(List<String> eventHandlerIds) {
    IStorage spaceStorage = mock(IStorage.class);
    when(naksha.getSpaceStorage()).thenReturn(spaceStorage);
    IReadSession readSession = mock(IReadSession.class);
    when(readSession.execute(argThat(anyReadHandlersRequest()))).thenReturn(new TestSuccessResult(eventHandlerIds));
    when(spaceStorage.newReadSession(any(NakshaContext.class), anyBoolean())).thenReturn(readSession);
  }

  private ArgumentMatcher<ReadFeatures> anyReadHandlersRequest() {
    return argument -> argument.getCollections().size() == 1 && argument.getCollections().get(0).equals(EVENT_HANDLERS);
  }

  static class TestSuccessResult extends SuccessResult {

    TestSuccessResult(List<String> ids) {
      XyzFeatureCodecFactory codecFactory = XyzFeatureCodecFactory.get();
      List<XyzFeatureCodec> featureCodecs = ids.stream()
          .map(id -> codecFactory.newInstance()
              .withOp(READ)
              .withId(id)
              .withFeature(new XyzFeature(id))
          )
          .toList();
      this.cursor = new HeapCacheCursor<>(codecFactory, featureCodecs, null);
    }
  }
}