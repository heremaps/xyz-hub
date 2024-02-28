package com.here.naksha.handler.activitylog;

import static com.here.naksha.handler.activitylog.ActivityLogRequestTranslationUtil.PREF_ACTIVITY_LOG_ID;
import static com.here.naksha.lib.core.models.storage.PRef.id;
import static com.here.naksha.lib.core.models.storage.PRef.uuid;

import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.POpType;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.test.common.assertions.POpAssertion;
import org.junit.jupiter.api.Test;

class ActivityLogRequestTranslationUtilTest {

  @Test
  void shouldTranslateIdToUuid() {
    // Given:
    String expectedId = "some_id";
    POp singleIdQuery = POp.eq(id(), expectedId);
    ReadFeatures readFeatures = new ReadFeatures().withPropertyOp(singleIdQuery);

    // When:
    ActivityLogRequestTranslationUtil.translatePropertyOperation(readFeatures);

    // Then:
    POpAssertion.assertThatOperation(readFeatures.getPropertyOp())
        .hasType(POpType.EQ)
        .hasPRef(uuid())
        .hasValue(expectedId);
  }

  @Test
  void shouldTranslateIdsToUuids() {
    // Given:
    String firstId = "id_1";
    String secondId = "id_2";
    POp idsQuery = POp.or(
        POp.eq(id(), firstId),
        POp.eq(id(), secondId)
    );
    ReadFeatures readFeatures = new ReadFeatures().withPropertyOp(idsQuery);

    // When:
    ActivityLogRequestTranslationUtil.translatePropertyOperation(readFeatures);

    // Then:
    POpAssertion.assertThatOperation(readFeatures.getPropertyOp())
        .hasType(POpType.OR)
        .hasChildrenThat(
            first -> first
                .hasType(POpType.EQ)
                .hasPRef(uuid())
                .hasValue(firstId),
            second -> second
                .hasType(POpType.EQ)
                .hasPRef(uuid())
                .hasValue(secondId)
        );
  }

  @Test
  void shouldTranslateActivityLogIdToId() {
    // Given:
    String expectedId = "some_id";
    POp singleActivityLogIdQuery = POp.eq(PREF_ACTIVITY_LOG_ID, expectedId);
    ReadFeatures readFeatures = new ReadFeatures().withPropertyOp(singleActivityLogIdQuery);

    // When:
    ActivityLogRequestTranslationUtil.translatePropertyOperation(readFeatures);

    // Then:
    POpAssertion.assertThatOperation(readFeatures.getPropertyOp())
        .hasType(POpType.EQ)
        .hasPRef(id())
        .hasValue(expectedId);
  }

  @Test
  void shouldTranslateActivityLogIdsToIds() {
    // Given:
    String firstId = "id_1";
    String secondId = "id_2";
    POp activityLogIdsQuery = POp.or(
        POp.eq(PREF_ACTIVITY_LOG_ID, firstId),
        POp.eq(PREF_ACTIVITY_LOG_ID, secondId)
    );
    ReadFeatures readFeatures = new ReadFeatures().withPropertyOp(activityLogIdsQuery);

    // When:
    ActivityLogRequestTranslationUtil.translatePropertyOperation(readFeatures);

    // Then:
    POpAssertion.assertThatOperation(readFeatures.getPropertyOp())
        .hasType(POpType.OR)
        .hasChildrenThat(
            first -> first
                .hasType(POpType.EQ)
                .hasPRef(id())
                .hasValue(firstId),
            second -> second
                .hasType(POpType.EQ)
                .hasPRef(id())
                .hasValue(secondId)
        );
  }

  @Test
  void shouldApplyMixedTranslations() {
    // Given:
    String id = "id";
    String activityLogId = "activity_log_id";
    POp mixedQuery = POp.or(
        POp.eq(id(), id),
        POp.eq(PREF_ACTIVITY_LOG_ID, activityLogId)
    );
    ReadFeatures readFeatures = new ReadFeatures().withPropertyOp(mixedQuery);

    // When:
    ActivityLogRequestTranslationUtil.translatePropertyOperation(readFeatures);

    // Then:
    POpAssertion.assertThatOperation(readFeatures.getPropertyOp())
        .hasType(POpType.OR)
        .hasChildrenThat(
            first -> first
                .hasType(POpType.EQ)
                .hasPRef(uuid())
                .hasValue(id),
            second -> second
                .hasType(POpType.EQ)
                .hasPRef(id())
                .hasValue(activityLogId)
        );
  }
}