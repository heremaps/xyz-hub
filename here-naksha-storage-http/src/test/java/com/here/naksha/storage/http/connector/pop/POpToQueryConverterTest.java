package com.here.naksha.storage.http.connector.pop;

import com.here.naksha.lib.core.models.payload.events.PropertyQueryAnd;
import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.PRef;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import com.here.naksha.lib.core.util.storage.RequestHelper;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static com.here.naksha.lib.core.models.storage.POp.*;
import static com.here.naksha.storage.http.connector.pop.POpToPropertiesQuery.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class POpToQueryConverterTest {

  @Test
  void andSingle() {
    POp pOp = and(
            eq(propRef("prop_1"), "1")
    );

    PropertyQueryAnd query = toPoPQueryAnd(pOp);

    assertQueryEquals( """
            [
            {"key":"property.prop_1","operation":"EQUALS","values":["1"]}
            ]""",
            query);
  }


    @Test
    void andDiffProp() {
      POp pOp = and(
              eq(propRef("prop_1"), "1"),
              eq(propRef("prop_2"), 2)
      );

      PropertyQueryAnd query = toPoPQueryAnd(pOp);

      assertQueryEquals( """
              [
              {"key":"property.prop_1","operation":"EQUALS","values":["1"]},
              {"key":"property.prop_2","operation":"EQUALS","values":[2]}
              ]""",
              query);
    }

    @Test
    void andSameProp() {
      POp pOp = and(
              eq(propRef("prop_1"), "1"),
              eq(propRef("prop_1"), "2")
      );

      PropertyQueryAnd query = toPoPQueryAnd(pOp);

      assertQueryEquals("""
               [
               {"key":"property.prop_1","operation":"EQUALS","values":["1"]},
               {"key":"property.prop_1","operation":"EQUALS","values":["2"]}
               ]""",
              query);
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

      PropertyQueryAnd query = toPoPQueryAnd(pOp);

      assertQueryEquals(
              """
               [
               {"key":"property.prop_1","operation":"EQUALS","values":["1"]},
               {"key":"property.prop_2","operation":"EQUALS","values":["2"]},
               {"key":"property.prop_3","operation":"EQUALS","values":["3"]},
               {"key":"property.prop_4","operation":"EQUALS","values":["4"]},
               {"key":"property.prop_5","operation":"EQUALS","values":["5"]}
               ]""",
              query);
    }

  void assertQueryEquals(String expectedJson, PropertyQueryAnd actualQuery) {
    assertEquals(
            expectedJson.replace(System.lineSeparator(), ""),
            JsonSerializable.serialize(actualQuery)
    );
  }


    @Test
    void orSingle() {
      POp pOp = or(
              eq(propRef("prop_1"), "1")
      );

      PropertyQueryAnd query = toPoPQueryAnd(pOp);

      assertQueryEquals("""
              [
              {"key":"property.prop_1","operation":"EQUALS","values":["1"]}
              ]""",
              query);
    }

    @Test
    void orSameProp() {
      POp pOp = or(
              eq(propRef("prop_1"), "1"),
              eq(propRef("prop_1"), "2")
      );

      PropertyQueryAnd query = toPoPQueryAnd(pOp);

      assertQueryEquals("""
              [
              {"key":"property.prop_1","operation":"EQUALS","values":["1","2"]}
              ]""",
              query);
    }

    @Test
    void orDiffProp_throw() {
      POp pOp = or(
              eq(propRef("prop_1"), "1"),
              eq(propRef("prop_2"), "2")
      );

      assertThrows(
              POpToQueryConversionException.class,
              () -> toPoPQueryAnd(pOp),
              "Operator OR with dwo different keys: property.prop_1 and property.prop_2"
      );
    }

    @Test
    void orIncompatibleOps_throw() {
      POp pOp = or(
              eq(propRef("prop_1"), 1),
              gt(propRef("prop_2"), 2)
      );

      assertThrows(
              POpToQueryConversionException.class,
              () -> toPoPQueryAnd(pOp),
              "Operators EQUALS and GREATER_THAN combined in one OR"
      );
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

      PropertyQueryAnd query = toPoPQueryAnd(pOp);

      assertQueryEquals("""
              [
              {"key":"property.prop_1","operation":"EQUALS","values":["1","2","3","4","5"]}
              ]""",
              query);
    }

    @Test
    void nullOrValue() {
      POp pOp = or(
              eq(propRef("prop_1"), 1),
              not(exists(propRef("prop_1")))
      );

      PropertyQueryAnd query = toPoPQueryAnd(pOp);

      assertQueryEquals("""
              [
              {"key":"property.prop_1","operation":"EQUALS","values":[1,null]}
              ]""",
              query);
    }

    @Test
    void equals() {
      POp pOp = eq(propRef("prop_1"), "1");

      PropertyQueryAnd query = toPoPQueryAnd(pOp);


      assertQueryEquals("""
              [
              {"key":"property.prop_1","operation":"EQUALS","values":["1"]}
              ]""",
              query);
    }

    @Test
    void notEquals() {
      POp pOp = not(eq(propRef("prop_1"), "1"));

      PropertyQueryAnd query = toPoPQueryAnd(pOp);

      assertQueryEquals("""
              [
              {"key":"property.prop_1","operation":"NOT_EQUALS","values":["1"]}
              ]""",
              query);
    }

    @ParameterizedTest
    @MethodSource("getOpsIncompatibleWithNot")
    void notWithIncompatibleOperation_throw(POp incompatibleOp) {
      POp pOp = not(incompatibleOp);

      assertThrows(POpToQueryConversionException.class, () -> toPoPQueryAnd(pOp));
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

      PropertyQueryAnd query = toPoPQueryAnd(pOp);

      assertQueryEquals("""
              [
              {"key":"property.prop_1","operation":"NOT_EQUALS","values":[null]}
              ]""",
              query);
    }

    @Test
    void notExistsSingle() {
      POp pOp = not(exists(propRef("prop_1")));

      PropertyQueryAnd query = toPoPQueryAnd(pOp);

      assertQueryEquals("""
              [
              {"key":"property.prop_1","operation":"EQUALS","values":[null]}
              ]""",
              query);
    }

    @Test
    void containsJson() {
      String json = "{\"num\":1,\"str\":\"str1\",\"arr\":[1,2,3],\"obj\":{}}";
      POp pOp = contains(propRef("prop_1"), json);

      PropertyQueryAnd query = toPoPQueryAnd(pOp);

      assertQueryEquals("""
              [
              {"key":"property.prop_1","operation":"CONTAINS","values":["{\\"num\\":1,\\"str\\":\\"str1\\",\\"arr\\":[1,2,3],\\"obj\\":{}}"]}
              ]""",
              query);
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

      PropertyQueryAnd query = toPoPQueryAnd(pOp);

      assertQueryEquals(
              """
               [
               {"key":"property.prop_1","operation":"EQUALS","values":[1]},
               {"key":"property.prop_2","operation":"GREATER_THAN","values":[2]},
               {"key":"property.prop_3","operation":"GREATER_THAN_OR_EQUALS","values":[3]},
               {"key":"property.prop_4","operation":"LESS_THAN","values":[4]},
               {"key":"property.prop_5","operation":"LESS_THAN_OR_EQUALS","values":[5]}
               ]""",
              query);
    }

    @ParameterizedTest
    @MethodSource("getNotSupportedOps")
    void notSupportedOps_throw(POp notSupportedOp) {
      POp pOp = not(notSupportedOp);

      assertThrows(POpToQueryConversionException.class, () -> toPoPQueryAnd(pOp));
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
    void dontAddPrefixToIdProp() {
      POp pOp = and(
              eq(RequestHelper.pRefFromPropPath(new String[]{"id"}), "1")
      );
      PropertyQueryAnd query = toPoPQueryAnd(pOp);

      assertQueryEquals(
              """
               [
               {"key":"id","operation":"EQUALS","values":["1"]}
               ]""",
              query);
    }

  private static @NotNull PRef propRef(String propName) {
    return RequestHelper.pRefFromPropPath(new String[]{"property", propName});
  }
}