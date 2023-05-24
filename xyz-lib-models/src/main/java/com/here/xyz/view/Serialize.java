package com.here.xyz.view;

/**
 * All serialization views.
 */
public interface Serialize {

  /**
   * Serialize for the public REST API.
   */
  interface Public extends Serialize, View.Export.Public {

  }

  /**
   * Serialize for the public REST API for a user with access to protected properties.
   */
  interface Protected extends Serialize, View.Export.Public, View.Export.Protected {

  }

  /**
   * Serialize for a trusted services.
   */
  interface Private extends Serialize, View.Export.Public, View.Export.Protected, View.Export.Private {

  }

  /**
   * Serialized for storage.
   */
  interface Store extends Serialize, View.Store {

  }

  /**
   * Serialize any, should not be used, except for special purpose.
   */
  interface Any extends Serialize, View.Export.Public, View.Export.Protected, View.Export.Private, View.Store {

  }

}
