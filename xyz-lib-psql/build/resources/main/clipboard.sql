{
  "id": "utm-dev:maptask",
  "owner": "ANONYMOUS",
  "title": "UTM MapTask - Dev",
  "storage": {
    "id": "naksha-view",
    "params": {
        "layers": [
            {"spaceId":"base"}, // "foo", "bar"
            {"spaceId":"delta"} // "foo"
        ]
    }
  },
  "createdAt": 1677751608448,
  "updatedAt": 1677751608448,
  "enableUUID": true,
  "description": "UTM MapTask space for Dev environment",
  "contentUpdatedAt": 1678796283705,
  "volatilityAtLastContentUpdate": 0.0000000000000002220446049250313
}

// only for bounding box queries
-> fetch area (from all in parallel)
-> calculate missing
-> fetch missing (from all in parallel)
-> merge results

//
-> fetch
-> merge results
