package com.here.naksha.lib.core.common;

import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.view.ViewDeserialize;
import com.here.naksha.lib.core.view.ViewSerialize;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

public class TestUtil {
    public static String loadFileOrFail(final @NotNull String rootPath, final @NotNull String fileName) {
        try {
            String json = new String(Files.readAllBytes(Paths.get(rootPath + fileName)));
            return json;
        } catch (IOException e) {
            Assertions.fail("Unable to read test file " + fileName, e);
            return null;
        }
    }

    public static <T> T parseJson(final @NotNull String jsonStr, final @NotNull Class<T> type) {
        T obj = null;
        try (final Json json = Json.get()) {
            obj = json.reader(ViewDeserialize.Storage.class).forType(type).readValue(jsonStr);
        } catch (Exception ex) {
            Assertions.fail("Unable tor parse jsonStr " + jsonStr, ex);
            return null;
        }
        return obj;
    }

    public static String toJson(final @NotNull Object obj) {
        String jsonStr = null;
        try (final Json json = Json.get()) {
            jsonStr = json.writer(ViewSerialize.Storage.class).writeValueAsString(obj);
        } catch (Exception ex) {
            throw unchecked(ex);
        }
        return jsonStr;
    }

}
