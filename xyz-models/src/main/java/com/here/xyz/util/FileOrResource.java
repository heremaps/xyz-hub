package com.here.xyz.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A file that is read either from disk or resources.
 *
 * @param <SELF> this type.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class FileOrResource<SELF extends FileOrResource<SELF>> {

  protected FileOrResource(@Nonnull String filename) {
    this(filename, null);
  }

  protected FileOrResource(@Nonnull String filename, @Nullable String searchPath) {
    this.filename = filename;
    this.searchPath = searchPath;
  }

  protected String filename;
  protected String searchPath;

  @Nullable
  public String searchPath() {
    return searchPath;
  }

  @Nonnull
  @SuppressWarnings("unchecked")
  public SELF withSearchPath(@Nullable String searchPath) {
    this.searchPath = searchPath;
    return (SELF) this;
  }

  @Nonnull
  public String filename() {
    return filename;
  }

  @SuppressWarnings("unchecked")
  @Nonnull
  public SELF withFilename(@Nonnull String filename) {
    final char firstChar = filename.charAt(0);
    if (firstChar != '/' && firstChar != '\\') {
      this.filename = '/' + filename;
    } else {
      this.filename = filename;
    }
    return (SELF) this;
  }

  @Nonnull
  public InputStream open() throws NullPointerException, IOException {
    return open(searchPath());
  }

  @Nonnull
  public InputStream open(@Nullable String searchPath) throws NullPointerException, IOException {
    String filePath = null;
    try {
      final Path path = (searchPath != null ? Paths.get(searchPath, filename) : Paths.get(filename)).toAbsolutePath();
      final File file = path.toFile();
      if (file.exists() && file.isFile()) {
        filePath = path.toString();
      } else {
        // Try to load from resources (JAR).
        URL url = getClass().getResource(filename);
        if (url == null) {
          final char firstChar = filename.charAt(0);
          if (firstChar != '/' && firstChar != '\\') {
            url = getClass().getResource('/' + filename);
          }
        }
        filePath = url != null ? url.getPath() : null;
      }
    } catch (final Throwable t) {
      error("Exception while opening file: " + filename, t);
    }

    if (filePath != null) {
      try {
        final InputStream in;
        final int jarEnd = filePath.indexOf(".jar!");
        if (filePath.startsWith("file:") && jarEnd > 0) {
          final String jarPath = filePath.substring("file:".length(), jarEnd + ".jar".length());
          filePath = filePath.substring(jarEnd + ".jar!".length());
          if (filePath.startsWith("/")) {
            filePath = filePath.substring(1);
          }
          info("Load file from JAR, jar-path: " + jarPath + ", file-path: " + filePath);
          //noinspection resource
          final JarFile jarFile = new JarFile(new File(jarPath), false, ZipFile.OPEN_READ);
          final ZipEntry entry = jarFile.getEntry(filePath);
          if (entry != null && entry.isDirectory()) {
            throw new IOException("The file is a directory");
          }
          if (entry == null) {
            throw new FileNotFoundException(filePath);
          }
          in = jarFile.getInputStream(entry);
        } else {
          info("Load file from disk: " + filePath);
          in = Files.newInputStream(new File(filePath).toPath());
        }
        return in;
      } catch (IOException e) {
        throw e;
      } catch (Throwable t) {
        error("Exception while opening file: " + filename, t);
      }
    }
    throw new FileNotFoundException("Failed to find file: " + filename);
  }

  public static byte[] readAllBytes(final @Nonnull InputStream in) throws IOException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final byte[] buffer = new byte[1024];
    int read;
    while ((read = in.read(buffer, 0, buffer.length)) >= 0) {
      out.write(buffer, 0, read);
    }
    out.flush();
    return out.toByteArray();
  }

  public byte[] readAllBytes() throws IOException {
    try (final InputStream in = open()) {
      return readAllBytes(in);
    }
  }

  /**
   * A method that can be overridden to send information to a logger.
   *
   * @param message the message to log as information.
   */
  protected void info(String message) {
  }

  /**
   * A method that can be overridden to send errors to a logger.
   *
   * @param message the message to log.
   * @param cause   the cause.
   */
  protected void error(String message, Throwable cause) {
  }
}