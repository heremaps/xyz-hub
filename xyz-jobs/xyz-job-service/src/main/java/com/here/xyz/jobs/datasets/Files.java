/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz.jobs.datasets;

import com.here.xyz.jobs.datasets.files.FileInputSettings;

public class Files<T extends Files> extends DatasetDescription implements FileBasedTarget<T> {
  FileOutputSettings outputSettings = new FileOutputSettings();
  FileInputSettings inputSettings = new FileInputSettings();

  public FileOutputSettings getOutputSettings() {
    return outputSettings;
  }

  public void setOutputSettings(FileOutputSettings outputSettings) {
    this.outputSettings = outputSettings;
  }

  public T withOutputSettings(FileOutputSettings outputSettings) {
    setOutputSettings(outputSettings);
    return (T) this;
  }

  public FileInputSettings getInputSettings() {
    return inputSettings;
  }

  public void setInputSettings(FileInputSettings inputSettings) {
    this.inputSettings = inputSettings;
  }

  public T withInputSettings(FileInputSettings inputSettings) {
    setInputSettings(inputSettings);
    return (T) this;
  }

  @Override
  public String getKey() {
    //No specific key to search for.
    return null;
  }
}
