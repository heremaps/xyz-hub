package com.here.xyz.httpconnector.util.jobs.datasets;

import com.here.xyz.models.hub.Ref;

public interface VersionRefSource<T extends VersionRefSource> {

  Ref getVersionRef();

  void setVersionRef(Ref versionRef);

  T withVersionRef(Ref versionRef);
}
