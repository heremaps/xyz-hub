package com.here.xyz.util;

import java.util.Locale;

public class DhString 
{ public static final String format(String format, Object... args) { return String.format(Locale.US,format,args); } 
}
