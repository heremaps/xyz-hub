package com.here.xyz.httpconnector.util.jobs.datasets.files;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.xyz.Typed;
import com.here.xyz.httpconnector.util.jobs.datasets.files.Partitioning.FeatureKey;
import com.here.xyz.httpconnector.util.jobs.datasets.files.Partitioning.Tiles;

@JsonSubTypes({
    @JsonSubTypes.Type(value = Tiles.class, name = "Tiles"),
    @JsonSubTypes.Type(value = FeatureKey.class, name = "FeatureKey")
})
public abstract class Partitioning implements Typed {

  public static class Tiles extends Partitioning {
    private int level = 12;
    private boolean clip;

    public int getLevel() {
      return level;
    }

    public void setLevel(int level) {
      this.level = level;
    }

    public Tiles withLevel(int level) {
      setLevel(level);
      return this;
    }

    public boolean isClip() {
      return clip;
    }

    public void setClip(boolean clip) {
      this.clip = clip;
    }

    public Tiles withClip(boolean clip) {
      setClip(clip);
      return this;
    }
  }

  public static class FeatureKey extends Partitioning {
    private String key = "id";

    public String getKey() {
      return key;
    }

    public void setKey(String key) {
      this.key = key;
    }

    public FeatureKey withKey(String key) {
      setKey(key);
      return this;
    }
  }
}
