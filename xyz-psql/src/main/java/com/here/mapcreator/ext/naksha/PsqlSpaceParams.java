package com.here.mapcreator.ext.naksha;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.xyz.XyzSerializable;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PsqlSpaceParams {

  public static @NotNull PsqlSpaceParams of(@Nullable Map<@NotNull String, @Nullable Object> map) {
    if (map == null) {
      return new PsqlSpaceParams();
    }
    return XyzSerializable.fromAnyMap(map, PsqlSpaceParams.class);
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public String tableName;

  public @NotNull String getTableName(@NotNull String spaceId) {
    return tableName != null && tableName.length() > 0 ? tableName : spaceId;
  }
}