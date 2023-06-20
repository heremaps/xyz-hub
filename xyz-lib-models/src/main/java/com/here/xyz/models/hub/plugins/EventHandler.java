package com.here.xyz.models.hub.plugins;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.IEventHandler;
import com.here.xyz.INaksha;
import com.here.xyz.models.geojson.implementation.Feature;
import java.lang.reflect.InvocationTargetException;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * Configuration of an event handler. There are two types of event handler: Local and Remote.
 *
 * <p>A local event handlers are embedded in Naksha-Hub and only have a {@link #className}, but no {@link #remote} property. Local
 * guarantees optimal performance, but has the disadvantage that it can't be deployed independently, and the handler can't use individual or
 * proprietary libraries, all code of the handler must be open source, maintained within the Naksha repository.
 *
 * <p>
 *
 * <p>The {@link EventHandler#properties} can be used as custom event handler configuration, if necessary.
 */
@AvailableSince(INaksha.v2_0)
@JsonTypeName(value = "EventHandler")
@JsonSubTypes({
    @JsonSubTypes.Type(value = RemoteEventHandler.class),
    @JsonSubTypes.Type(value = Connector.class)
})
public class EventHandler extends Feature implements IPlugin<IEventHandler> {

  @AvailableSince(INaksha.v2_0)
  public static final String CLASS_NAME = "className";

  /**
   * Create a new local extension.
   *
   * @param id        the identifier of the event handler.
   * @param className the full qualified name of the class to load.
   */
  @AvailableSince(INaksha.v2_0)
  @JsonCreator
  public EventHandler(
      @JsonProperty(ID) @NotNull String id,
      @JsonProperty(CLASS_NAME) @NotNull String className
  ) {
    super(id);
    this.className = className;
  }

  /**
   * The classname to load, the class must implement the {@link IEventHandler} interface, and the constructor must accept exactly one
   * parameter of the type {@link Connector}. It may throw any exception.
   */
  @AvailableSince(INaksha.v2_0)
  @JsonProperty(CLASS_NAME)
  public @NotNull String className;

  /**
   * Whether this connector is active. If set to false, the handler will not be added into the event pipelines of spaces. So all spaces
   * using this connector will bypass this connector. If the connector configures the storage, all requests to spaces using the connector as
   * storage will fail.
   */
  @JsonProperty
  private boolean active;

  @Override
  public @NotNull IEventHandler newInstance() throws Exception {
    try {
      return PluginCache.newInstance(className, IEventHandler.class, this);
    } catch (InvocationTargetException ite) {
      if (ite.getCause() instanceof Exception e) {
        throw e;
      }
      throw ite;
    }
  }
}