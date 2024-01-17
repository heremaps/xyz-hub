package com.here.naksha.lib.core.common.assertions;

import com.here.naksha.lib.core.models.payload.events.QueryDelimiter;
import com.here.naksha.lib.core.models.payload.events.QueryOperation;
import com.here.naksha.lib.core.models.payload.events.QueryParameter;
import com.here.naksha.lib.core.util.ValueList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.junit.jupiter.api.Assertions.*;

public class QueryParameterAssertion {

    final private @NotNull QueryParameter param;
    private QueryParameterAssertion(final @NotNull QueryParameter param) {
        this.param = param;
    }

    public static QueryParameterAssertion assertThat(final @Nullable QueryParameter param) {
        assertNotNull(param);
        return new QueryParameterAssertion(param);
    }

    public QueryParameterAssertion hasKey(final @NotNull String key) {
        assertEquals(key, param.key(), "Param key doesn't match");
        return this;
    }

    public QueryParameterAssertion hasOperation(final @NotNull QueryOperation op) {
        assertEquals(op, param.op(), "Param operation doesn't match");
        return this;
    }

    public QueryParameterAssertion hasValueSize(int size) {
        assertEquals(size, param.values().size(), "Count of values doesn't match");
        return this;
    }

    public QueryParameterAssertion hasValueDelimiterSize(int size) {
        assertEquals(size, param.valuesDelimiter().size(), "Count of delimiters doesn't match");
        return this;
    }

    public <T> QueryParameterAssertion hasValues(T... values) {
        assertArrayEquals(values, param.values().toArray(), "Supplied list of values don't match");
        return this;
    }

    public QueryParameterAssertion hasValueDelimiters(QueryDelimiter... delimiters) {
        assertArrayEquals(delimiters, param.valuesDelimiter().toArray(), "Supplied list of delimiters don't match");
        return this;
    }

}
