package com.here.xyz.util;

import java.util.Locale;

public class DhString 
{private static final Locale en = new Locale("en", "US");   
 public static final String format(String format, Object... args) { return String.format(en,format,args); }
}
