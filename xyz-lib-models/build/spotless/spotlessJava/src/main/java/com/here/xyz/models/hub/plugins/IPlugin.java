package com.here.xyz.models.hub.plugins;


import org.jetbrains.annotations.NotNull;

/**
 * A plugin is some code that implements an API and can be instantiated.
 *
 * @param <API> the api-type to create by the configuration.
 */
public interface IPlugin<API> {

  /**
   * Create a new instance of the plugin.
   *
   * @return the API.
   * @throws Exception if creating the instance failed.
   */
  @NotNull API newInstance() throws Exception;
}
