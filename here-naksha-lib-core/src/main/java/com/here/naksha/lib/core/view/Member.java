package com.here.naksha.lib.core.view;

// See: https://www.baeldung.com/jackson-json-view-annotation

import com.fasterxml.jackson.annotation.JsonView;
import com.here.naksha.lib.core.INaksha;
import org.jetbrains.annotations.ApiStatus.AvailableSince;

/**
 * Members that are not annotated for special visibility conditions, will always be part of the serialization and deserialization. When
 * members annotated with the {@link JsonView} annotation, then they have some distinct capabilities. How they are exported to clients or
 * how they are imported from clients, and if they are persisted into a storage.
 *
 * <p><b>Note</b>: Control can be fine-grained, so to make a virtual read-only property, that can only be read by clients, but does not
 * exist in the database nor is available otherwise internally, do the following: <pre>{@code
 * @JsonView({Member.Export.User.class, Member.Export.Manager.class})
 * @JsonGetter("foo")
 * public @NotNull String foo() {
 *   return "foo";
 * }
 * }</pre>
 * <p>
 * This will result in the property being part of the public REST API, but the client can't set it (no import rule), and it is not
 * serialized internally or stored in the database. It is recommended to not add the {@link JsonView} annotation, except some special
 * handling needed. In that case, whenever possible, use the default {@link Member.User}, {@link Member.Manager} or {@link Member.Internal}
 * annotations, which declare import and export rules. Only for special cases do use individual fine-grained annotations.
 */
@SuppressWarnings("unused")
@AvailableSince(INaksha.v2_0_3)
public interface Member {

  /**
   * The member can be read and written by all authenticated users, is stored in the storage and available for hashing.
   */
  @AvailableSince(INaksha.v2_0_3)
  interface User extends Export.User, Import.User, Storage, Hashing {

  }

  /**
   * The member can only be read and written by managers, is stored in the storage and available for hashing.
   */
  @AvailableSince(INaksha.v2_0_3)
  interface Manager extends Export.Manager, Import.Manager, Storage, Hashing {

  }

  /**
   * The member can only be read and written by internal trusted components, is stored in the storage and available for hashing.
   */
  @AvailableSince(INaksha.v2_0_3)
  interface Internal extends Export.Internal, Import.Internal, Storage, Hashing {

  }

  /**
   * Fine-grained serialization control.
   */
  @AvailableSince(INaksha.v2_0_3)
  interface Export {

    /**
     * The member is visible to users.
     */
    @AvailableSince(INaksha.v2_0_3)
    interface User extends Export {

    }

    /**
     * The member is visible for managers.
     */
    @AvailableSince(INaksha.v2_0_3)
    interface Manager extends Export {

    }

    /**
     * The member is visible for internal components.
     */
    @AvailableSince(INaksha.v2_0_3)
    interface Internal extends Export {

    }

    /**
     * The member is visible for users, managers and internally.
     */
    @AvailableSince(INaksha.v2_0_3)
    interface All extends User, Manager, Internal {

    }
  }

  /**
   * Fine-grained deserialization control.
   */
  @AvailableSince(INaksha.v2_0_3)
  interface Import {

    /**
     * Read the member from user input.
     */
    @AvailableSince(INaksha.v2_0_3)
    interface User extends Import {

    }

    /**
     * Read the member from manager input.
     */
    @AvailableSince(INaksha.v2_0_3)
    interface Manager extends Import {

    }

    /**
     * Read the member from internal components.
     */
    @AvailableSince(INaksha.v2_0_3)
    interface Internal extends Import {

    }

    /**
     * Read the member from users, managers and internal components.
     */
    @AvailableSince(INaksha.v2_0_3)
    interface All extends User, Manager, Internal {

    }
  }

  /**
   * The member is present in the storage.
   */
  @AvailableSince(INaksha.v2_0_3)
  interface Storage {

  }

  /**
   * If the member should be part of a serialization done for hashing.
   */
  @AvailableSince(INaksha.v2_0_3)
  interface Hashing {

  }
}