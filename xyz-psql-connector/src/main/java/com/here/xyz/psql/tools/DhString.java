package com.here.xyz.psql.tools;

import java.util.Locale;

public class DhString
{ public static final String format(String format, Object... args) { return String.format(Locale.US,format,args); }
}
