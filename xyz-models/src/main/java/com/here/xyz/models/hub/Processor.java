package com.here.xyz.models.hub;

import com.here.xyz.IEventHandler;
import java.util.concurrent.ConcurrentHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Pseudo enumeration about all known processors. Need to be extended by the runtime (xyz-hub-service).
 */
public class Processor {

  /**
   * Read-only concurrent map with all known processors.
   */
  public static final ConcurrentHashMap<String, Processor> all = new ConcurrentHashMap<>();

  /**
   * Adds the given processor to the list of known processors, so that {@link #get(String)} returns it. Adding the same processor multiple
   * times will not have any effect.
   *
   * @param processor the processor to add.
   * @return the added processor.
   * @throws IllegalArgumentException if another processor with the same identifier is already registered.
   */
  public static @NotNull Processor add(@NotNull Processor processor) {
    final Processor existing = all.putIfAbsent(processor.id, processor);
    if (existing != null && existing != processor) {
      throw new IllegalArgumentException(
          "A processor with the id " + processor.id + " exists already, class is: " + processor.processorClass);
    }
    return processor;
  }

  /**
   * Returns the processor for the given identifier.
   *
   * @param id the identifier to lookup.
   * @return the processor or {@code null}, if no such processor known.
   */
  public static @Nullable Processor get(@Nullable String id) {
    if (id == null) {
      return null;
    }
    return all.get(id);
  }

  /**
   * The PSQL processor.
   */
  public static final Processor PsqlProcessor = add(new Processor("psql", "com.here.xyz.psql.PsqlProcessor"));

  /**
   * The View processor.
   */
  public static final Processor ViewProcessor = add(new Processor("view", "TBD"));

  /**
   * The HTTP processor.
   */
  public static final Processor HttpProcessor = add(new Processor("http", "TBD"));

  /**
   * Create a new processor type.
   *
   * @param id        the identifier for this processor.
   * @param className the name of the class to late load.
   */
  public Processor(@NotNull String id, @NotNull String className) {
    this.id = id;
    this.className = className;
  }

  /**
   * Create a new processor type.
   *
   * @param id             the identifier for this processor.
   * @param processorClass the class implementing this processor.
   */
  public Processor(@NotNull String id, @NotNull Class<? extends IEventHandler> processorClass) {
    this.id = id;
    this.processorClass = processorClass;
    this.className = processorClass.getName();
  }

  /**
   * The type identifier of the processor, used in “type” of the {@link Connector#processor}.
   */
  public final @NotNull String id;

  /**
   * The full qualified class-name.
   */
  public final @NotNull String className;

  /**
   * The processor class, will be bound by the application to the concrete implementation.
   */
  private Class<? extends IEventHandler> processorClass;

  /**
   * Returns the class implementing this processor.
   *
   * @return the class implementing this processor.
   */
  public @NotNull Class<? extends IEventHandler> getProcessorClass() {
    Class<? extends IEventHandler> processorClass = this.processorClass;
    if (processorClass == null) {
      try {
        //noinspection unchecked
        this.processorClass = processorClass = (Class<? extends IEventHandler>) Class.forName(className);
      } catch (ClassNotFoundException e) {
        throw new Error("Process implementation requested, but class not found", e);
      }
    }
    return processorClass;
  }

  /**
   * Binds the given class as implementation. Should be done at runtime to ensure that the compiler knows the class at compile time,
   * otherwise a risk is there that the class can't be loaded later, when needed.
   *
   * @param processorClass the class to bind.
   */
  public void setProcessorClass(@NotNull Class<? extends IEventHandler> processorClass) {
    this.processorClass = processorClass;
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    return this == o;
  }

  @Override
  public @NotNull String toString() {
    return id;
  }
}
