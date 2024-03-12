package com.here.naksha.storage.http;

import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.PRef;
import com.here.naksha.lib.core.util.storage.RequestHelper;
import com.here.naksha.storage.http.POpToQueryConverter.POpToQueryConversionException;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URLEncoder;

import static com.here.naksha.lib.core.models.storage.POp.*;
import static com.here.naksha.storage.http.POpToQueryConverter.p0pToQuery;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class POpToQueryConverterTest {

  @Test
  void andSingle() {
    POp pOp = and(
            eq(propRef("prop_1"), "1")
    );

    String query = p0pToQuery(pOp);

    assertEquals(query, "property.prop_1=1");
  }

  @Test
  void andDiffProp() {
    POp pOp = and(
            eq(propRef("prop_1"), "1"),
            eq(propRef("prop_2"), "2")
    );

    String query = p0pToQuery(pOp);

    assertEquals(query, "property.prop_1=1&property.prop_2=2");
  }

  @Test
  void andSameProp() {
    POp pOp = and(
            eq(propRef("prop_1"), "1"),
            eq(propRef("prop_1"), "2")
    );

    String query = p0pToQuery(pOp);

    assertEquals(query, "property.prop_1=1&property.prop_1=2");
  }

  @Test
  void andManyChildren() {
    POp pOp = and(
            eq(propRef("prop_1"), "1"),
            eq(propRef("prop_2"), "2"),
            eq(propRef("prop_3"), "3"),
            eq(propRef("prop_4"), "4"),
            eq(propRef("prop_5"), "5")
    );

    String query = p0pToQuery(pOp);

    assertEquals(query, "property.prop_1=1&property.prop_2=2&property.prop_3=3&property.prop_4=4&property.prop_5=5");
  }

  private static @NotNull PRef propRef(String propName) {
    return RequestHelper.pRefFromPropPath(new String[]{"property", propName});
  }

  @Test
  void orSingle() {
    POp pOp = or(
            eq(propRef("prop_1"), "1")
    );

    String query = p0pToQuery(pOp);

    assertEquals(query, "property.prop_1=1");
  }

  @Test
  void orSameProp() {
    POp pOp = or(
            eq(propRef("prop_1"), "1"),
            eq(propRef("prop_1"), "2")
    );

    String query = p0pToQuery(pOp);

    assertEquals(query, "property.prop_1=1,2");
  }

  @Test
  void orDiffProp_throw() {
    POp pOp = or(
            eq(propRef("prop_1"), "1"),
            eq(propRef("prop_2"), "2")
    );

    assertThrows(POpToQueryConversionException.class, () -> p0pToQuery(pOp));
  }

  @Test
  void orIncompatibleOps_throw() {
    POp pOp = or(
            eq(propRef("prop_1"), 1),
            gt(propRef("prop_2"), 2)
    );

    assertThrows(POpToQueryConversionException.class, () -> p0pToQuery(pOp));
  }

  @Test
  void orManyChildren() {
    POp pOp = or(
            eq(propRef("prop_1"), "1"),
            eq(propRef("prop_1"), "2"),
            eq(propRef("prop_1"), "3"),
            eq(propRef("prop_1"), "4"),
            eq(propRef("prop_1"), "5")
    );

    String query = p0pToQuery(pOp);

    assertEquals(query, "property.prop_1=1,2,3,4,5");
  }

  @Test
  void nullOrValue() {
    POp pOp = or(
            eq(propRef("prop_1"), 1),
            not(exists(propRef("prop_1")))
    );

    String query = p0pToQuery(pOp);

    assertEquals(query, "property.prop_1=1,.null");
  }

  @Test
  void equals() {
    POp pOp = eq(propRef("prop_1"), "1");

    String query = p0pToQuery(pOp);

    assertEquals(query, "property.prop_1=1");
  }

  @Test
  void notEquals() {
    POp pOp = not(eq(propRef("prop_1"), "1"));

    String query = p0pToQuery(pOp);

    assertEquals(query, "property.prop_1!=1");
  }

  @ParameterizedTest
  @MethodSource("getOpsIncompatibleWithNot")
  void notWithIncompatibleOperation_throw(POp incompatibleOp) {
    POp pOp = not(incompatibleOp);

    assertThrows(POpToQueryConversionException.class, () -> p0pToQuery(pOp));
  }

  public static POp[] getOpsIncompatibleWithNot() {
    return new POp[]{
            or(gt(propRef("prop_1"), 1)),
            and(eq(propRef("prop_1"), "1")),
            gt(propRef("prop_1"), 1),
            gte(propRef("prop_1"), 1),
            lt(propRef("prop_1"), 1),
            lte(propRef("prop_1"), 1),
            contains(propRef("prop_1"), "{}")
    };
  }

  @Test
  void existsSingle() {
    POp pOp = exists(propRef("prop_1"));

    String query = p0pToQuery(pOp);

    assertEquals(query, "property.prop_1!=.null");
  }

  @Test
  void notExistsSingle() {
    POp pOp = not(exists(propRef("prop_1")));

    String query = p0pToQuery(pOp);

    assertEquals(query, "property.prop_1=.null");
  }

  @Test
  void containsJson() {
    String json = "{\"num\":1,\"str\":\"str1\",\"arr\":[1,2,3],\"obj\":{}}";
    POp pOp = contains(propRef("prop_1"), json);

    String query = p0pToQuery(pOp);

    assertEquals(query, "property.prop_1=cs=" + urlEncoded(json));
  }

  @Test
  void simpleLeafOperations() {
    POp pOp = and(
            eq(propRef("prop_1"), 1),
            gt(propRef("prop_2"), 2),
            gte(propRef("prop_3"), 3),
            lt(propRef("prop_4"), 4),
            lte(propRef("prop_5"), 5)
    );

    String query = p0pToQuery(pOp);

    assertEquals(query,
            "property.prop_1=1" +
                    "&property.prop_2=gt=2" +
                    "&property.prop_3=gte=3" +
                    "&property.prop_4=lt=4" +
                    "&property.prop_5=lte=5");
  }

  @ParameterizedTest
  @MethodSource("getNotSupportedOps")
  void notSupportedOps_throw(POp notSupportedOp) {
    POp pOp = not(notSupportedOp);

    assertThrows(POpToQueryConversionException.class, () -> p0pToQuery(pOp));
  }

  public static POp[] getNotSupportedOps() {
    PRef prop1Ref = propRef("prop_1");
    return new POp[]{
            startsWith(prop1Ref, "1"),
            isNull(prop1Ref),
            isNotNull(prop1Ref),
    };
  }

  @Test
  void translateIdProp() {
    POp pOp = and(
            eq(RequestHelper.pRefFromPropPath(new String[]{"f","id"}), "1")
    );
    String query = p0pToQuery(pOp);

    assertEquals(query, "f.id=1");
  }

  public static String urlEncoded(String text) {
    return URLEncoder.encode(text, UTF_8);
  }
}