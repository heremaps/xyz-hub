package com.here.xyz.util;


import com.here.xyz.util.json.JsonSerializable;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Singleton with IO helpers. */
public final class IoHelp {

    /**
     * Read a resource from the JAR.
     *
     * @param resource The resource name, which should start with a slash.
     * @return The input stream to the resource.
     * @throws IOException If any error occurred.
     */
    public static @NotNull InputStream openResource(@NotNull String resource) throws IOException {
        final InputStream is = ClassLoader.getSystemResourceAsStream(resource);
        if (is == null) {
            throw new IOException("Resource " + resource + " not found");
        }
        return is;
    }

    /**
     * Read a resource from the JAR.
     *
     * @param resource The resource name, which should start with a slash.
     * @return The resource as string.
     * @throws IOException If any error occurred.
     */
    public static @NotNull String readResource(@NotNull String resource) throws IOException {
        final InputStream is = ClassLoader.getSystemResourceAsStream(resource);
        assert is != null;
        try (final BufferedReader buffer = new BufferedReader(new InputStreamReader(is))) {
            return buffer.lines().collect(Collectors.joining("\n"));
        }
    }

    /**
     * Read a file from the resources.
     *
     * @param filename The filename of the file to read, e.g. "foo.json".
     * @return The loaded file.
     * @throws IOException If the file does not exist or any other error occurred.
     */
    public static byte @NotNull [] readResourceBytes(@NotNull String filename) throws IOException {
        final URL url = ClassLoader.getSystemResource(filename);
        if (url == null) {
            throw new FileNotFoundException(filename);
        }
        try (final InputStream is = url.openStream()) {
            return readNBytes(is, Integer.MAX_VALUE);
        }
    }

    private static final int DEFAULT_BUFFER_SIZE = 8192;
    private static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8;

    /**
     * Reads up to a specified number of bytes from the input stream. This method blocks until the
     * requested number of bytes has been read, end of stream is detected, or an exception is thrown.
     * This method does not close the input stream.
     *
     * <p>The length of the returned array equals the number of bytes read from the stream. If {@code
     * len} is zero, then no bytes are read and an empty byte array is returned. Otherwise, up to
     * {@code len} bytes are read from the stream. Fewer than {@code len} bytes may be read if end of
     * stream is encountered.
     *
     * <p>When this stream reaches end of stream, further invocations of this method will return an
     * empty byte array.
     *
     * <p>Note that this method is intended for simple cases where it is convenient to read the
     * specified number of bytes into a byte array. The total amount of memory allocated by this
     * method is proportional to the number of bytes read from the stream which is bounded by {@code
     * len}. Therefore, the method may be safely called with very large values of {@code len} provided
     * sufficient memory is available.
     *
     * <p>The behavior for the case where the input stream is <i>asynchronously closed</i>, or the
     * thread interrupted during the read, is highly input stream specific, and therefore not
     * specified.
     *
     * <p>If an I/O error occurs reading from the input stream, then it may do so after some, but not
     * all, bytes have been read. Consequently the input stream may not be at end of stream and may be
     * in an inconsistent state. It is strongly recommended that the stream be promptly closed if an
     * I/O error occurs.
     *
     * @param is the input stream to read from.
     * @param len the maximum number of bytes to read
     * @return a byte array containing the bytes read from this input stream
     * @throws IllegalArgumentException if {@code length} is negative
     * @throws IOException if an I/O error occurs
     * @throws OutOfMemoryError if an array of the required size cannot be allocated.
     */
    public static byte @NotNull [] readNBytes(final @NotNull InputStream is, int len) throws IOException {
        if (len < 0) {
            throw new IllegalArgumentException("len < 0");
        }

        List<byte[]> bufs = null;
        byte[] result = null;
        int total = 0;
        int remaining = len;
        int n;
        do {
            byte[] buf = new byte[Math.min(remaining, DEFAULT_BUFFER_SIZE)];
            int nread = 0;

            // read to EOF which may read more or less than buffer size
            while ((n = is.read(buf, nread, Math.min(buf.length - nread, remaining))) > 0) {
                nread += n;
                remaining -= n;
            }

            if (nread > 0) {
                if (MAX_BUFFER_SIZE - total < nread) {
                    throw new OutOfMemoryError("Required array size too large");
                }
                if (nread < buf.length) {
                    buf = Arrays.copyOfRange(buf, 0, nread);
                }
                total += nread;
                if (result == null) {
                    result = buf;
                } else {
                    if (bufs == null) {
                        bufs = new ArrayList<>();
                        bufs.add(result);
                    }
                    bufs.add(buf);
                }
            }
            // if the last call to read returned -1 or the number of bytes
            // requested have been read then break
        } while (n >= 0 && remaining > 0);

        if (bufs == null) {
            if (result == null) {
                return new byte[0];
            }
            return result.length == total ? result : Arrays.copyOf(result, total);
        }

        result = new byte[total];
        int offset = 0;
        remaining = total;
        for (byte[] b : bufs) {
            int count = Math.min(b.length, remaining);
            System.arraycopy(b, 0, result, offset, count);
            offset += count;
            remaining -= count;
        }

        return result;
    }

    /**
     * The loaded bytes.
     *
     * @param path The path loaded.
     * @param bytes The bytes loaded.
     */
    public record LoadedBytes(@NotNull String path, byte @NotNull [] bytes) {}

    /**
     * The loaded config.
     *
     * @param path The path loaded.
     * @param config The loaded config.
     * @param <CONFIG> The config-type.
     */
    public record LoadedConfig<CONFIG>(@NotNull String path, CONFIG config) {}

    /**
     * Read a file either from the given search paths or when not found there from
     * "~/.config/{appName}", and eventually from the resources.
     *
     * @param filename The filename of the file to read, e.g. "auth/jwt.key".
     * @param tryWorkingDirectory If the filename should be used relative to the working directory (or
     *     as absolute file path).
     * @param appName The name of the application, when searching the default location
     *     (~/.config/{appName}).
     * @param searchPaths Optional paths to search along, before trying the default location.
     * @return The loaded configuration file.
     * @throws IOException If the file does not exist or any other error occurred.
     */
    public static <CONFIG> @NotNull LoadedConfig<CONFIG> readConfigFromHomeOrResource(
            @NotNull String filename,
            boolean tryWorkingDirectory,
            @NotNull String appName,
            @NotNull Class<CONFIG> configClass,
            @Nullable String... searchPaths)
            throws IOException {
        final LoadedBytes loadedBytes =
                readBytesFromHomeOrResource(filename, tryWorkingDirectory, appName, searchPaths);
        final CONFIG config = JsonSerializable.deserialize(loadedBytes.bytes, configClass);
        return new LoadedConfig<>(loadedBytes.path, config);
    }

    /**
     * Read a file either from the given search paths or when not found there from
     * "~/.config/{appName}", and eventually from the resources.
     *
     * @param filename The filename of the file to read, e.g. "auth/jwt.key".
     * @param tryWorkingDirectory If the filename should be used relative to the working directory (or
     *     as absolute file path).
     * @param appName The name of the application, when searching the default location
     *     (~/.config/{appName}).
     * @param searchPaths Optional paths to search along, before trying the default location.
     * @return The loaded configuration file.
     * @throws IOException If the file does not exist or any other error occurred.
     */
    public static @NotNull LoadedBytes readBytesFromHomeOrResource(
            @NotNull String filename,
            boolean tryWorkingDirectory,
            @NotNull String appName,
            @Nullable String... searchPaths)
            throws IOException {
        //noinspection ConstantConditions
        if (filename == null) {
            throw new FileNotFoundException("null");
        }

        Path filePath;
        if (tryWorkingDirectory) {
            filePath = Paths.get(filename).toAbsolutePath();
            final File file = filePath.toFile();
            if (file.exists() && file.isFile() && file.canRead()) {
                return new LoadedBytes(filePath.toString(), Files.readAllBytes(filePath));
            }
        }

        final char first = filename.charAt(0);
        if (first == '/' || first == '\\') {
            filename = filename.substring(1);
        }

        for (final String path : searchPaths) {
            if (path == null) {
                continue;
            }
            filePath = Paths.get(path).toAbsolutePath();
            final File file = filePath.toFile();
            if (file.exists() && file.isFile() && file.canRead()) {
                return new LoadedBytes(filePath.toString(), Files.readAllBytes(filePath));
            }
        }
        final String userHome = System.getProperty("user.home");
        if (userHome != null) {
            filePath = Paths.get(userHome, ".config", appName, filename).toAbsolutePath();
        } else {
            filePath = null;
        }
        if (filePath != null) {
            final File file = filePath.toFile();
            if (file.exists() && file.isFile() && file.canRead()) {
                return new LoadedBytes(filePath.toString(), Files.readAllBytes(filePath));
            }
        }

        final URL url = ClassLoader.getSystemResource(filename);
        if (url == null) {
            throw new FileNotFoundException(filename);
        }
        try (final InputStream is = url.openStream()) {
            return new LoadedBytes(url.toString(), readNBytes(is, Integer.MAX_VALUE));
        }
    }

    /**
     * Formatting helper.
     *
     * @param format The format string.
     * @param args The arguments.
     * @return The formatted string.
     */
    public static @NotNull String format(@NotNull String format, @Nullable Object... args) {
        return String.format(Locale.US, format, args);
    }

    /**
     * Simplified toString implementation.
     *
     * @param o The object to stringify.
     * @return The stringified version.
     */
    public static @NotNull String asString(@Nullable Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString();
    }

    /**
     * Parses the given value.
     *
     * @param value The value to parse.
     * @param type The class of the return-type.
     * @param <T> The type to return.
     * @return The parsed value, or the default value.
     * @throws IllegalArgumentException If the value does not match the expected type or is {@code
     *     null}.
     */
    public static <T> @NotNull T parseValue(@Nullable Object value, @NotNull Class<T> type) {
        final T v = parseNullableValue(value, type, null, false);
        if (v == null) {
            throw new IllegalArgumentException("Value must not be null");
        }
        return v;
    }

    /**
     * Parses the given value.
     *
     * @param value The value to parse.
     * @param type The class of the return-type.
     * @param defaultValue The default value, when no value given.
     * @param ignoreInvalid if {@code false}, than an {@link IllegalArgumentException} is thrown, if
     *     the value is of an invalid type.
     * @param <T> The type to return.
     * @return The parsed value, or the default value.
     * @throws IllegalArgumentException If {@code ignoreInvalid} if {@code false} and the value does
     *     not match the expected type.
     */
    public static <T> @NotNull T parseValue(
            @Nullable Object value, @NotNull Class<T> type, @NotNull T defaultValue, boolean ignoreInvalid) {
        final T v = parseNullableValue(value, type, defaultValue, ignoreInvalid);
        assert v != null;
        return v;
    }

    /**
     * Parses the given value.
     *
     * @param value The value to parse.
     * @param type The class of the return-type.
     * @param defaultValue The default value, when no value given.
     * @param ignoreInvalid if {@code false}, than an {@link IllegalArgumentException} is thrown, if
     *     the value is of an invalid type.
     * @param <T> The type to return.
     * @return The parsed value, or the default value.
     * @throws IllegalArgumentException If {@code ignoreInvalid} if {@code false} and the value does
     *     not match the expected type.
     */
    public static <T> @Nullable T parseNullableValue(
            @Nullable Object value, @NotNull Class<T> type, @Nullable T defaultValue, boolean ignoreInvalid) {
        if (value == null) {
            return defaultValue;
        }
        if (type.isAssignableFrom(value.getClass())) {
            return type.cast(value);
        }

        if (value instanceof String string) {
            if (type == Boolean.class) {
                if ("true".equalsIgnoreCase(string)) {
                    return type.cast(Boolean.TRUE);
                }
                if ("false".equalsIgnoreCase(string)) {
                    return type.cast(Boolean.FALSE);
                }
                if (ignoreInvalid) {
                    return defaultValue;
                }
                throw new IllegalArgumentException("Invalid type, expected boolean");
            }
            if (type == Byte.class) {
                try {
                    return type.cast(Byte.parseByte(string));
                } catch (NumberFormatException e) {
                    if (ignoreInvalid) {
                        return defaultValue;
                    }
                    throw new IllegalArgumentException("Invalid type, expected byte");
                }
            }
            if (type == Short.class) {
                try {
                    return type.cast(Short.parseShort(string));
                } catch (NumberFormatException e) {
                    if (ignoreInvalid) {
                        return defaultValue;
                    }
                    throw new IllegalArgumentException("Invalid type, expected short");
                }
            }
            if (type == Integer.class) {
                try {
                    return type.cast(Integer.parseInt(string));
                } catch (NumberFormatException e) {
                    if (ignoreInvalid) {
                        return defaultValue;
                    }
                    throw new IllegalArgumentException("Invalid type, expected int");
                }
            }
            if (type == Long.class) {
                try {
                    return type.cast(Long.parseLong(string));
                } catch (NumberFormatException e) {
                    if (ignoreInvalid) {
                        return defaultValue;
                    }
                    throw new IllegalArgumentException("Invalid type, expected long");
                }
            }
            if (type == Float.class) {
                try {
                    return type.cast(Float.parseFloat(string));
                } catch (NumberFormatException e) {
                    if (ignoreInvalid) {
                        return defaultValue;
                    }
                    throw new IllegalArgumentException("Invalid type, expected float");
                }
            }
            if (type == Double.class) {
                try {
                    return type.cast(Double.parseDouble(string));
                } catch (NumberFormatException e) {
                    if (ignoreInvalid) {
                        return defaultValue;
                    }
                    throw new IllegalArgumentException("Invalid type, expected double");
                }
            }
        }
        if (value instanceof Number number) {
            if (type == Byte.class) {
                return type.cast(number.byteValue());
            }
            if (type == Short.class) {
                return type.cast(number.shortValue());
            }
            if (type == Integer.class) {
                return type.cast(number.intValue());
            }
            if (type == Long.class) {
                return type.cast(number.longValue());
            }
            if (type == Float.class) {
                return type.cast(number.floatValue());
            }
            if (type == Double.class) {
                return type.cast(number.doubleValue());
            }
        }

        if (ignoreInvalid) {
            return defaultValue;
        }
        throw new IllegalArgumentException("Invalid type, expected " + type.getName());
    }
}
