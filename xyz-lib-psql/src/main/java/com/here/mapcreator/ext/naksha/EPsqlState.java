package com.here.mapcreator.ext.naksha;


import com.here.naksha.lib.core.util.ILike;
import com.here.naksha.lib.core.util.StringHelper;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** The PSQL error states. */
public class EPsqlState implements ILike, CharSequence {
    // https://www.postgresql.org/docs/current/errcodes-appendix.html

    private static final ConcurrentHashMap<String, EPsqlState> all = new ConcurrentHashMap<>();

    protected EPsqlState(@NotNull String value) {
        this(value, true);
    }

    protected EPsqlState(@NotNull String value, boolean store) {
        if (store) {
            all.put(value, this);
        }
        this.value = value;
    }

    /** The value of the state. */
    public final @NotNull String value;

    /**
     * Returns the state of the given SQLException.
     *
     * @param e the exception for which to return the {@link EPsqlState}.
     * @return the {@link EPsqlState}.
     */
    public static @NotNull EPsqlState of(@NotNull SQLException e) {
        return of(e.getSQLState());
    }

    /**
     * Returns the state for the given text.
     *
     * @param text the text for which to return the {@link EPsqlState}.
     * @return the {@link EPsqlState}.
     */
    public static @NotNull EPsqlState of(@NotNull String text) {
        final EPsqlState state = all.get(text);
        return state != null ? state : new EPsqlState(text, false);
    }

    public static final EPsqlState OK = new EPsqlState("00000");
    public static final EPsqlState TOO_MANY_RESULTS = new EPsqlState("0100E");
    public static final EPsqlState NO_DATA = new EPsqlState("02000");
    public static final EPsqlState INVALID_PARAMETER_TYPE = new EPsqlState("07006");

    /**
     * We could establish a connection with the server for unknown reasons. Could be a network
     * problem.
     */
    public static final EPsqlState CONNECTION_UNABLE_TO_CONNECT = new EPsqlState("08001");

    public static final EPsqlState CONNECTION_DOES_NOT_EXIST = new EPsqlState("08003");

    /**
     * The server rejected our connection attempt. Usually an authentication failure, but could be a
     * configuration error like asking for a SSL connection with a server that wasn't built with SSL
     * support.
     */
    public static final EPsqlState CONNECTION_REJECTED = new EPsqlState("08004");

    /** After a connection has been established, it went bad. */
    public static final EPsqlState CONNECTION_FAILURE = new EPsqlState("08006");

    public static final EPsqlState CONNECTION_FAILURE_DURING_TRANSACTION = new EPsqlState("08007");

    /**
     * The server sent us a response the driver was not prepared for and is either bizarre datastream
     * corruption, a driver bug, or a protocol violation on the server's part.
     */
    public static final EPsqlState PROTOCOL_VIOLATION = new EPsqlState("08P01");

    public static final EPsqlState COMMUNICATION_ERROR = new EPsqlState("08S01");
    public static final EPsqlState NOT_IMPLEMENTED = new EPsqlState("0A000");
    public static final EPsqlState DATA_ERROR = new EPsqlState("22000");
    public static final EPsqlState STRING_DATA_RIGHT_TRUNCATION = new EPsqlState("22001");
    public static final EPsqlState NUMERIC_VALUE_OUT_OF_RANGE = new EPsqlState("22003");
    public static final EPsqlState BAD_DATETIME_FORMAT = new EPsqlState("22007");
    public static final EPsqlState DATETIME_OVERFLOW = new EPsqlState("22008");
    public static final EPsqlState DIVISION_BY_ZERO = new EPsqlState("22012");
    public static final EPsqlState MOST_SPECIFIC_TYPE_DOES_NOT_MATCH = new EPsqlState("2200G");
    public static final EPsqlState INVALID_PARAMETER_VALUE = new EPsqlState("22023");
    public static final EPsqlState NOT_NULL_VIOLATION = new EPsqlState("23502");
    public static final EPsqlState FOREIGN_KEY_VIOLATION = new EPsqlState("23503");
    public static final EPsqlState UNIQUE_VIOLATION = new EPsqlState("23505");
    public static final EPsqlState CHECK_VIOLATION = new EPsqlState("23514");
    public static final EPsqlState EXCLUSION_VIOLATION = new EPsqlState("23P01");
    public static final EPsqlState INVALID_CURSOR_STATE = new EPsqlState("24000");
    public static final EPsqlState TRANSACTION_STATE_INVALID = new EPsqlState("25000");
    public static final EPsqlState ACTIVE_SQL_TRANSACTION = new EPsqlState("25001");
    public static final EPsqlState NO_ACTIVE_SQL_TRANSACTION = new EPsqlState("25P01");
    public static final EPsqlState IN_FAILED_SQL_TRANSACTION = new EPsqlState("25P02");
    public static final EPsqlState INVALID_SQL_STATEMENT_NAME = new EPsqlState("26000");
    public static final EPsqlState INVALID_AUTHORIZATION_SPECIFICATION = new EPsqlState("28000");
    public static final EPsqlState INVALID_PASSWORD = new EPsqlState("28P01");
    public static final EPsqlState INVALID_TRANSACTION_TERMINATION = new EPsqlState("2D000");
    public static final EPsqlState STATEMENT_NOT_ALLOWED_IN_FUNCTION_CALL = new EPsqlState("2F003");
    public static final EPsqlState INVALID_SAVEPOINT_SPECIFICATION = new EPsqlState("3B000");
    public static final EPsqlState SERIALIZATION_FAILURE = new EPsqlState("40001");
    public static final EPsqlState DEADLOCK_DETECTED = new EPsqlState("40P01");
    public static final EPsqlState SYNTAX_ERROR = new EPsqlState("42601");
    public static final EPsqlState UNDEFINED_COLUMN = new EPsqlState("42703");
    public static final EPsqlState UNDEFINED_OBJECT = new EPsqlState("42704");
    public static final EPsqlState WRONG_OBJECT_TYPE = new EPsqlState("42809");
    public static final EPsqlState NUMERIC_CONSTANT_OUT_OF_RANGE = new EPsqlState("42820");
    public static final EPsqlState DATA_TYPE_MISMATCH = new EPsqlState("42821");
    public static final EPsqlState UNDEFINED_FUNCTION = new EPsqlState("42883");
    public static final EPsqlState INVALID_NAME = new EPsqlState("42602");
    public static final EPsqlState DATATYPE_MISMATCH = new EPsqlState("42804");
    public static final EPsqlState CANNOT_COERCE = new EPsqlState("42846");
    public static final EPsqlState UNDEFINED_TABLE = new EPsqlState("42P01");
    public static final EPsqlState OUT_OF_MEMORY = new EPsqlState("53200");
    public static final EPsqlState OBJECT_NOT_IN_STATE = new EPsqlState("55000");
    public static final EPsqlState OBJECT_IN_USE = new EPsqlState("55006");
    public static final EPsqlState QUERY_CANCELED = new EPsqlState("57014");
    public static final EPsqlState SYSTEM_ERROR = new EPsqlState("60000");
    public static final EPsqlState IO_ERROR = new EPsqlState("58030");
    public static final EPsqlState UNEXPECTED_ERROR = new EPsqlState("99999");
    public static final EPsqlState DUPLICATE_TABLE = new EPsqlState("42P07");
    public static final EPsqlState DUPLICATE_OBJECT = new EPsqlState("42710");

    public boolean isConnectionError() {
        return this == CONNECTION_UNABLE_TO_CONNECT
                || this == CONNECTION_DOES_NOT_EXIST
                || this == CONNECTION_REJECTED
                || this == CONNECTION_FAILURE
                || this == CONNECTION_FAILURE_DURING_TRANSACTION;
    }

    @Override
    public int length() {
        return value.length();
    }

    @Override
    public char charAt(int index) {
        return value.charAt(index);
    }

    @Override
    public @NotNull CharSequence subSequence(int start, int end) {
        return value.subSequence(start, end);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof EPsqlState other) {
            return value.equals(other.value);
        }
        return false;
    }

    @Override
    public @NotNull String toString() {
        return value;
    }

    @Override
    public boolean isLike(@Nullable Object other) {
        return equals(other) || (other instanceof CharSequence chars && StringHelper.equals(value, chars));
    }
}
