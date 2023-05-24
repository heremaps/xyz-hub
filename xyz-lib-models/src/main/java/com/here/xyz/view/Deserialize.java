package com.here.xyz.view;

/**
 * All deserialization views.
 */
public interface Deserialize {

  /**
   * Deserialized from the public REST API.
   */
  interface Public extends Deserialize, View.Import.Public {

  }

  /**
   * Deserialize from public REST API with the client having access to protected properties.
   */
  interface Protected extends Deserialize, View.Import.Public, View.Import.Protected {

  }

  /**
   * Deserialize from a trusted component.
   */
  interface Private extends Deserialize, View.Import.Public, View.Import.Protected, View.Import.Private {

  }

  /**
   * Deserialize from the database.
   */
  interface Store extends Deserialize, View.Store {

  }

  /**
   * Deserialize any, should not be used, except for special purpose.
   */
  interface Any extends Deserialize, View.Import.Public, View.Import.Protected, View.Import.Private, View.Store {

  }
}
