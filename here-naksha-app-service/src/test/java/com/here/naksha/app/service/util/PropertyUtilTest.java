package com.here.naksha.app.service.util;

import com.here.naksha.app.service.http.ops.PropertyUtil;
import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.payload.events.QueryParameterList;
import com.here.naksha.lib.core.models.storage.OpType;
import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.POpType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import static com.here.naksha.app.common.TestUtil.urlEncoded;
import static com.here.naksha.app.common.assertions.POpAssertion.assertThatOperation;
import static com.here.naksha.lib.core.models.storage.OpType.NOT;
import static com.here.naksha.lib.core.models.storage.OpType.OR;
import static com.here.naksha.lib.core.models.storage.POpType.*;
import static com.here.naksha.lib.core.models.storage.POpType.CONTAINS;
import static com.here.naksha.lib.core.models.storage.PRef.ID_PROP_PATH;
import static com.here.naksha.lib.core.models.storage.PRef.TAGS_PROP_PATH;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class PropertyUtilTest {

    @Test
    void testBuildOperationForPropertySearchParams() {
        final QueryParameterList params = new QueryParameterList(
                urlEncoded("f.id")+"="+urlEncoded("@value:1")+",'12345'"
                        + "&p.prop_2!=value_2,value_22"
                        + "&p.prop_3=.null,value_33"
                        + "&p.prop_4!=.null,value_44"
                        + "&p.prop_5>=5.5,55"
                        + "&west=-180"
                        + "&p.prop_6<=6,66"
                        + "&p.prop_7>7,77"
                        + "&tags=one,two"
                        + "&p.prop_8<8,88"
                        + "&p.array_1@>"+ urlEncoded("@element_1") + ",element_2"
                        + "&p.prop_10=gte=555,5555"
                        + "&p.prop_11=lte=666,6666"
                        + "&p.prop_12=gt=777,7777"
                        + "&p.prop_13=lt=888,8888"
                        + "&"+ urlEncoded("properties.@ns:com:here:xyz.tags") + "=cs=" + urlEncoded("{\"id\":\"123\"}") + ",element_4"
        );
        final Set<String> excludedKeys = Set.of("west","tags");


        final POp op = PropertyUtil.buildOperationForPropertySearchParams(params);
        assertThatOperation(op).hasType(OpType.AND);

        final List<POp> opList = op.children();

        // ensure there are total 14 operations
        assertNotNull(opList, "Expected multiple AND operations");
        assertEquals(14, opList.size(), "Expected total 14 AND operations");

        int innerOpsInd = 0;
        // validate operation 1
        assertThatOperation(opList.get(innerOpsInd++))
                .hasType(POpType.OR)
                .hasChildrenThat(
                        first -> first.hasType(EQ).hasPRefWithPath(ID_PROP_PATH).hasValue("@value:1"),
                        second -> second.hasType(EQ).hasPRefWithPath(ID_PROP_PATH).hasValue("12345")
                )
        ;
        // validate operation 2
        assertThatOperation(opList.get(innerOpsInd++))
                .hasType(POpType.OR)
                .hasChildrenThat(
                        first -> first.hasType(NOT)
                                .hasChildrenThat(
                                        f1 -> f1.hasType(EQ).hasNonIndexedPRefWithPath(new String[]{"properties","prop_2"}).hasValue("value_2")
                                ),
                        second -> second.hasType(NOT)
                                .hasChildrenThat(
                                        s1 -> s1.hasType(EQ).hasNonIndexedPRefWithPath(new String[]{"properties","prop_2"}).hasValue("value_22")
                                )
                )
        ;
        // validate operation 3
        assertThatOperation(opList.get(innerOpsInd++))
                .hasType(POpType.OR)
                .hasChildrenThat(
                        first -> first.hasType(NOT)
                                .hasChildrenThat(
                                        f1 -> f1.hasType(EXISTS).hasNonIndexedPRefWithPath(new String[]{"properties","prop_3"})
                                ),
                        second -> second.hasType(EQ).hasNonIndexedPRefWithPath(new String[]{"properties","prop_3"}).hasValue("value_33")
                )
        ;
        // validate operation 4
        assertThatOperation(opList.get(innerOpsInd++))
                .hasType(POpType.OR)
                .hasChildrenThat(
                        first -> first.hasType(EXISTS).hasNonIndexedPRefWithPath(new String[]{"properties","prop_4"}),
                        second -> second.hasType(NOT)
                                .hasChildrenThat(
                                        f1 -> f1.hasType(EQ).hasNonIndexedPRefWithPath(new String[]{"properties","prop_4"}).hasValue("value_44")
                                )
                )
        ;
        // validate operation 5
        assertThatOperation(opList.get(innerOpsInd++))
                .hasType(POpType.OR)
                .hasChildrenThat(
                        first -> first.hasType(GTE).hasNonIndexedPRefWithPath(new String[]{"properties","prop_5"}).hasValue(5.5),
                        second -> second.hasType(GTE).hasNonIndexedPRefWithPath(new String[]{"properties","prop_5"}).hasValue(55L)
                )
        ;
        // validate operation 6
        assertThatOperation(opList.get(innerOpsInd++))
                .hasType(POpType.OR)
                .hasChildrenThat(
                        first -> first.hasType(LTE).hasNonIndexedPRefWithPath(new String[]{"properties","prop_6"}).hasValue(6L),
                        second -> second.hasType(LTE).hasNonIndexedPRefWithPath(new String[]{"properties","prop_6"}).hasValue(66L)
                )
        ;
        // validate operation 7
        assertThatOperation(opList.get(innerOpsInd++))
                .hasType(POpType.OR)
                .hasChildrenThat(
                        first -> first.hasType(GT).hasNonIndexedPRefWithPath(new String[]{"properties","prop_7"}).hasValue(7L),
                        second -> second.hasType(GT).hasNonIndexedPRefWithPath(new String[]{"properties","prop_7"}).hasValue(77L)
                )
        ;
        // validate operation 8
        assertThatOperation(opList.get(innerOpsInd++))
                .hasType(POpType.OR)
                .hasChildrenThat(
                        first -> first.hasType(LT).hasNonIndexedPRefWithPath(new String[]{"properties","prop_8"}).hasValue(8L),
                        second -> second.hasType(LT).hasNonIndexedPRefWithPath(new String[]{"properties","prop_8"}).hasValue(88L)
                )
        ;
        // validate operation 9
        assertThatOperation(opList.get(innerOpsInd++))
                .hasType(POpType.OR)
                .hasChildrenThat(
                        first -> first.hasType(CONTAINS).hasNonIndexedPRefWithPath(new String[]{"properties","array_1"}).hasValue("@element_1"),
                        second -> second.hasType(CONTAINS).hasNonIndexedPRefWithPath(new String[]{"properties","array_1"}).hasValue("element_2")
                )
        ;
        // validate operation 10
        assertThatOperation(opList.get(innerOpsInd++))
                .hasType(POpType.OR)
                .hasChildrenThat(
                        first -> first.hasType(GTE).hasNonIndexedPRefWithPath(new String[]{"properties","prop_10"}).hasValue(555L),
                        second -> second.hasType(GTE).hasNonIndexedPRefWithPath(new String[]{"properties","prop_10"}).hasValue(5555L)
                )
        ;
        // validate operation 11
        assertThatOperation(opList.get(innerOpsInd++))
                .hasType(POpType.OR)
                .hasChildrenThat(
                        first -> first.hasType(LTE).hasNonIndexedPRefWithPath(new String[]{"properties","prop_11"}).hasValue(666L),
                        second -> second.hasType(LTE).hasNonIndexedPRefWithPath(new String[]{"properties","prop_11"}).hasValue(6666L)
                )
        ;
        // validate operation 12
        assertThatOperation(opList.get(innerOpsInd++))
                .hasType(POpType.OR)
                .hasChildrenThat(
                        first -> first.hasType(GT).hasNonIndexedPRefWithPath(new String[]{"properties","prop_12"}).hasValue(777L),
                        second -> second.hasType(GT).hasNonIndexedPRefWithPath(new String[]{"properties","prop_12"}).hasValue(7777L)
                )
        ;
        // validate operation 13
        assertThatOperation(opList.get(innerOpsInd++))
                .hasType(POpType.OR)
                .hasChildrenThat(
                        first -> first.hasType(LT).hasNonIndexedPRefWithPath(new String[]{"properties","prop_13"}).hasValue(888L),
                        second -> second.hasType(LT).hasNonIndexedPRefWithPath(new String[]{"properties","prop_13"}).hasValue(8888L)
                )
        ;
        // validate operation 14
        assertThatOperation(opList.get(innerOpsInd++))
                .hasType(POpType.OR)
                .hasChildrenThat(
                        first -> first.hasType(OR)
                                .hasChildrenThat(
                                        f1 -> f1.hasType(CONTAINS).hasPRefWithPath(TAGS_PROP_PATH).hasValue("{\"id\":\"123\"}"),
                                        f2 -> f2.hasType(CONTAINS).hasPRefWithPath(TAGS_PROP_PATH).hasValue("[{\"id\":\"123\"}]")
                                ),
                        second -> second.hasType(CONTAINS).hasPRefWithPath(TAGS_PROP_PATH).hasValue("element_4")
                )
        ;
    }

    private static Arguments propQuerySpec(String query, String assertionDesc) {
        return arguments(query, named(assertionDesc, XyzErrorException.class));
    }

    @ParameterizedTest
    @MethodSource("propQueriesWithException")
    void testKnownException(String queryString, Class<? extends Throwable> exceptionType) {
        assertThrowsExactly(exceptionType, () -> {
            final QueryParameterList queryParameters = new QueryParameterList(queryString);
            PropertyUtil.buildOperationForPropertySearchParams(queryParameters);
        });
    }

    private static Stream<Arguments> propQueriesWithException() {
        return Stream.of(
            // invalid delimiter
            propQuerySpec("p.prop_1=1+5", "Exception for invalid delimiter +"),
            // invalid operation on string value
            propQuerySpec("p.prop_1>string_value", "Exception for invalid string operation >"),
            propQuerySpec("p.prop_1<string_value", "Exception for invalid string operation <"),
            propQuerySpec("p.prop_1>=string_value", "Exception for invalid string operation >="),
            propQuerySpec("p.prop_1<=string_value", "Exception for invalid string operation <="),
            propQuerySpec("p.prop_1=gt=string_value", "Exception for invalid string operation =gt="),
            propQuerySpec("p.prop_1=lt=string_value", "Exception for invalid string operation =lt="),
            propQuerySpec("p.prop_1=gte=string_value", "Exception for invalid string operation =gte="),
            propQuerySpec("p.prop_1=lte=string_value", "Exception for invalid string operation =lte="),
            // invalid operation on boolean value
            propQuerySpec("p.prop_1>false", "Exception for invalid boolean operation >"),
            propQuerySpec("p.prop_1<false", "Exception for invalid boolean operation <"),
            propQuerySpec("p.prop_1>=false", "Exception for invalid boolean operation >="),
            propQuerySpec("p.prop_1<=false", "Exception for invalid boolean operation <="),
            propQuerySpec("p.prop_1=gt=false", "Exception for invalid boolean operation =gt="),
            propQuerySpec("p.prop_1=lt=false", "Exception for invalid boolean operation =lt="),
            propQuerySpec("p.prop_1=gte=false", "Exception for invalid boolean operation =gte="),
            propQuerySpec("p.prop_1=lte=false", "Exception for invalid boolean operation =lte=")
        );
    }

}
