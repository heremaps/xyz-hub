/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */
package com.here.xyz.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.datasets.DatasetDescription.Space;
import org.junit.jupiter.api.Test;

public class JobSourceFiltersTest {
  private static String exportJob(String filters) {
    return """
        {
          "source": {"type": "Space", "id": "my-space", "filters": %s},
          "target": {"type": "Files"}
        }
        """.formatted(filters);
  }

  @Test
  public void refInSourceFiltersIsRejected() {
    JsonProcessingException e = assertThrows(JsonProcessingException.class,
        () -> XyzSerializable.deserialize(exportJob("{\"ref\": \"2\"}"), Job.class));

    assertTrue(e.getMessage().contains("versionRef"), "Unexpected message: " + e.getMessage());
  }

  @Test
  public void versionRefOnTheSourceIsStillAccepted() throws JsonProcessingException {
    Job job = XyzSerializable.deserialize("""
        {
          "source": {"type": "Space", "id": "my-space", "versionRef": "2"},
          "target": {"type": "Files"}
        }
        """, Job.class);

    assertEquals("2", ((Space<?>) job.getSource()).getVersionRef().toString());
  }
}
