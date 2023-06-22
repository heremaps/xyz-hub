package com.here.naksha.lib.core.view;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.view.Member.Import;
import org.jetbrains.annotations.ApiStatus.AvailableSince;

/** All deserialization views. */
@AvailableSince(INaksha.v2_0_3)
@SuppressWarnings("unused")
public interface DeserializeView {

    /** Deserialized input from a user. */
    @AvailableSince(INaksha.v2_0_3)
    interface User extends DeserializeView, Import.User {}

    /** Deserialize input from a manager. */
    @AvailableSince(INaksha.v2_0_3)
    interface Manager extends DeserializeView, Import.User, Import.Manager {}

    /** Deserialize input from an internal component. */
    @AvailableSince(INaksha.v2_0_3)
    interface Internal extends DeserializeView, Import.User, Import.Manager, Import.Internal {}

    /** Deserialize from the database. */
    @AvailableSince(INaksha.v2_0_3)
    interface Storage extends DeserializeView, Member.Storage {}

    /** Deserialize all members, even those normally only for hashing purpose. */
    @AvailableSince(INaksha.v2_0_3)
    interface All extends DeserializeView, Import.User, Import.Manager, Import.Internal, Member.Storage, Member.Hashing {}
}