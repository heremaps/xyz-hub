package com.here.naksha.lib.psql.sql;

import java.util.Locale;

public class DhString {

    public static String format(String format, Object... args) {
        return String.format(Locale.US, format, args);
    }
}
