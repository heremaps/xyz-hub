/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

package com.here.xyz.util.di;

import com.here.xyz.util.di.ImplementationInstanceProvider.Implementable;

/**
 * A default implementation for {@link ImplementationProvider} which provides the ability to return an instance of
 * a class being an implementation which has been loaded (injected) at runtime.
 * In order to use the ImplementationInstanceProvider the implementation has to implement the tagging-interface {@link Implementable}.
 * By contract, implementing classes must provide a no-argument constructor. That constructor is the only one being used
 * by ImplementationInstanceProvider for instance creation but may be overridden in the actual implementation and its subclasses.
 * Other overloading constructors however will not be called by the ImplementationInstanceProvider.
 */
public abstract class ImplementationInstanceProvider<T extends Implementable> implements ImplementationProvider {

  private final Class<? super T> implementation;

  protected ImplementationInstanceProvider(Class<? super T> implementation) {
    this.implementation = implementation;
  }

  public T getInstance() {
    return (T) ImplementationProvider.loadProvider(this.getClass()).getInstance();
  }

  public interface Implementable {}
}
