package com.here.naksha.lib.core.exceptions;

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The base class for all exceptions thrown by a storage.
 */
@AvailableSince(NakshaVersion.v2_0_8)
public class StorageException extends RuntimeException {

  /**
   * Wrap the given error result into an exception.
   *
   * @param errorResult The error result to wrap.
   */
  @AvailableSince(NakshaVersion.v2_0_8)
  public StorageException(@NotNull ErrorResult errorResult) {
    super(errorResult.message);
    this.reason = errorResult.reason;
    this.errorResult = errorResult;
  }

  /**
   * Create a new storage exception with the given reason.
   *
   * @param reason The error reason.
   */
  @AvailableSince(NakshaVersion.v2_0_8)
  public StorageException(@NotNull XyzError reason) {
    super(reason.toString());
    this.reason = reason;
  }

  /**
   * Create a new storage exception with the given reason.
   *
   * @param message The error message.
   */
  @AvailableSince(NakshaVersion.v2_0_8)
  public StorageException(@NotNull String message) {
    super(message);
    this.reason = XyzError.get(message);
  }

  /**
   * Create a new storage exception with the given reason.
   *
   * @param message The error message.
   * @param cause   The cause.
   */
  @AvailableSince(NakshaVersion.v2_0_8)
  public StorageException(@NotNull String message, @Nullable Throwable cause) {
    super(message, cause);
    this.reason = XyzError.get(message);
  }

  /**
   * Create a new storage exception with the given reason.
   *
   * @param reason  The error reason.
   * @param message An arbitrary error message.
   */
  @AvailableSince(NakshaVersion.v2_0_8)
  public StorageException(@NotNull XyzError reason, @Nullable String message) {
    super(message == null ? reason.toString() : message);
    this.reason = reason;
  }

  /**
   * Create a new storage exception with the given reason.
   *
   * @param reason The error reason.
   * @param cause  The cause.
   */
  @AvailableSince(NakshaVersion.v2_0_8)
  public StorageException(@NotNull XyzError reason, @Nullable Throwable cause) {
    super(reason.toString(), cause);
    this.reason = reason;
  }

  /**
   * Create a new storage exception with the given reason.
   *
   * @param reason  The error reason.
   * @param message An arbitrary error message.
   * @param cause   The cause.
   */
  @AvailableSince(NakshaVersion.v2_0_8)
  public StorageException(@NotNull XyzError reason, @Nullable String message, @Nullable Throwable cause) {
    super(message == null ? reason.toString() : message, cause);
    this.reason = reason;
  }

  private final @NotNull XyzError reason;
  private @Nullable ErrorResult errorResult;

  /**
   * Convert this exception into an error-result.
   *
   * @return this exception converted into an error-result.
   */
  @AvailableSince(NakshaVersion.v2_0_8)
  public @NotNull ErrorResult toErrorResult() {
    if (errorResult == null) {
      errorResult = new ErrorResult(reason, getMessage());
    }
    return errorResult;
  }
}