package com.here.xyz;


import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.models.hub.plugins.Connector;
import com.here.xyz.models.payload.responses.XyzError;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Default event handler implementations used by Naksha. Basically, every connector will eventually
 * use one registered standard handler, bind it to arbitrary parameters and then add into a pipeline
 * to process events.
 */
@SuppressWarnings({"unused", "RedundantThrows"})
public abstract class OldEventHandler implements IEventHandler {

    /**
     * Creates a new event handler instance with the given parameters. Can only be added to one
     * pipeline, every pipeline requires a new instance of the handler.
     *
     * @param connector the connector configuration.
     * @throws XyzErrorException if initialization of the handler failed.
     */
    public OldEventHandler(@NotNull Connector connector) throws XyzErrorException {}

    /** Map with all known managed handlers. */
    @SuppressWarnings("rawtypes")
    private static final ConcurrentHashMap<String, Class> allClasses = new ConcurrentHashMap<>();

    @SuppressWarnings("rawtypes")
    private static final ConcurrentHashMap<String, Constructor> allConstructors = new ConcurrentHashMap<>();

    /**
     * Register the given event handler to the list of known standard handlers, so that {@link
     * #getClass(String)} returns it and {@link #newInstance(Connector)} can create a new instance of
     * it. Adding the same handler multiple times will not have any effect.
     *
     * @param id the identifier to register.
     * @param handlerClass the processor to add.
     * @param <H> The handler type.
     * @throws IllegalArgumentException if another handler with the same identifier exists, or the
     *     given class does not support the required constructor (the one with only a {@link Map} as
     *     parameter).
     */
    @SuppressWarnings("unchecked")
    public static synchronized <H extends OldEventHandler> void register(
            @NotNull String id, @NotNull Class<H> handlerClass) {
        final Constructor<H> constructor;
        try {
            constructor = handlerClass.getConstructor(Connector.class);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Missing constructor(@NotNull Connector connector)");
        }
        final Class<OldEventHandler> existing = allClasses.putIfAbsent(id, handlerClass);
        if (existing != null) {
            if (existing != handlerClass) {
                throw new IllegalArgumentException("The event handler with the id "
                        + id
                        + " exists already, class is: "
                        + existing.getSimpleName());
            }
            // Registering multiple times is ignored!
        } else {
            allConstructors.put(id, constructor);
        }
    }

    /**
     * Returns the class of the handler with the given identifier.
     *
     * @param id the identifier to lookup.
     * @param <H> The handler type.
     * @return the event handler class or {@code null}, if no such handler known.
     */
    @SuppressWarnings("unchecked")
    public static <H extends OldEventHandler> @Nullable Class<H> getClass(@Nullable String id) {
        if (id == null) {
            return null;
        }
        return allClasses.get(id);
    }

    /**
     * Creates a new event handler and binds it to the given parameters.
     *
     * @param connector the connector configuration to be instantiated.
     * @param <H> The handler type.
     * @return the event handler.
     * @throws XyzErrorException if the creation failed.
     */
    @SuppressWarnings("unchecked")
    public static <H extends OldEventHandler> @NotNull H newInstance(@NotNull Connector connector)
            throws XyzErrorException {
        final String id = connector.getId();
        //noinspection ConstantConditions
        if (id == null) {
            throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "The given connector does not have an 'id'");
        }
        final Class<H> handlerClass = allClasses.get(id);
        final Constructor<H> constructor = allConstructors.get(id);
        if (handlerClass == null || constructor == null) {
            throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "No such event handler (" + id + ") exists");
        }
        try {
            return constructor.newInstance(connector);
        } catch (Throwable t) {
            // Unwrap exceptions thrown by the constructor.
            if (t instanceof InvocationTargetException ite) {
                //noinspection AssignmentToCatchBlockParameter
                t = ite.getTargetException();
                if (t instanceof XyzErrorException) {
                    throw (XyzErrorException) t;
                }
                throw new XyzErrorException(
                        XyzError.ILLEGAL_ARGUMENT,
                        "Failed to create a new instance of event handler "
                                + id
                                + ", class "
                                + handlerClass.getName()
                                + ", msg: "
                                + t.getMessage(),
                        t);
            }
            throw new XyzErrorException(
                    XyzError.EXCEPTION,
                    "Failed to create a new instance of event handler "
                            + id
                            + ", class "
                            + handlerClass.getName()
                            + ", msg: "
                            + t.getMessage(),
                    t);
        }
    }
}