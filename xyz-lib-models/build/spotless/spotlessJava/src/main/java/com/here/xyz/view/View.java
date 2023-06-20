package com.here.xyz.view;

// See: https://www.baeldung.com/jackson-json-view-annotation


import com.fasterxml.jackson.annotation.JsonView;

/**
 * Properties that are not bound to views, will always be part of the serialization and
 * deserialization. When properties are linked to views using the {@link JsonView} annotation, then
 * they have three distinct capabilities. How they are exported to clients, how they are imported
 * from clients and if they are persisted into a storage.
 *
 * <p><b>Warning</b>: Control can be very fine grained, so to make a virtual read-only property,
 * that can only be read by clients, but does not exist in the database nor is available otherwise
 * internally, do this:
 *
 * <pre>{@code
 * @JsonView({Export.Public.class})
 * @JsonGetter("foo")
 * public @NotNull String foo() {
 *   return "foo";
 * }
 * }</pre>
 *
 * This will result in the property being part of the public REST API, but the client can't set it,
 * and it is not serialized internally. It is recommended to not add the {@link JsonView}
 * annotation, except some special handling needed. In that case, whenever possible, use the default
 * {@link View.Public}, {@link View.Protected} or {@link View.Private} annotations.
 */
@SuppressWarnings("unused")
public interface View {

  /** The property is public (for read and write) and persisted. */
  interface Public extends Export.Public, Import.Public, Store {}

  /** The property is protected (for read and write) and persisted. */
  interface Protected extends Export.Protected, Import.Protected, Store {}

  /** The property is private (for read and write) and persisted. */
  interface Private extends Export.Private, Import.Private, Store {}

  /** Defines the visibility of a property. */
  interface Export {

    /** The property is visible to everybody authenticated. */
    interface Public extends Export {}

    /** The property is only visible with special access rights. */
    interface Protected extends Export {}

    /** The property is only visible to trusted components. */
    interface Private extends Export {}
  }

  interface Import {

    /** The property can be modified normally. */
    interface Public extends Import {}

    /** The property can be modified only with special rights. */
    interface Protected extends Import {}

    /** The property can be modified only by trusted components. */
    interface Private extends Import {}
  }

  /** If the member is persisted in the storage. */
  interface Store {}
}
