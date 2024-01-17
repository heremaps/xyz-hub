package com.here.naksha.lib.core.util;

import com.here.naksha.lib.core.models.storage.NonIndexedPRef;
import com.here.naksha.lib.core.models.storage.PRef;
import com.here.naksha.lib.core.util.storage.RequestHelper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class RequestHelperTest {

    @Test
    public void testPRefFromStandardPath() {
        final PRef pref = RequestHelper.pRefFromPropPath(new String[]{"properties","@ns:com:here:xyz","tags"});
        assertNotNull(pref);
        assertFalse(pref instanceof NonIndexedPRef, "Must be instanceof PRef");
    }

    @Test
    public void testPRefFromNonStandardPath() {
        final PRef pref = RequestHelper.pRefFromPropPath(new String[]{"properties","prop_1"});
        assertNotNull(pref);
        assertTrue(pref instanceof NonIndexedPRef, "Must be instanceof NonIndexedPRef");
    }
}
