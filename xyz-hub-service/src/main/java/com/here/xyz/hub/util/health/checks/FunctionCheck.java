/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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
package com.here.xyz.hub.util.health.checks;

import static com.here.xyz.hub.util.health.schema.Status.Result.ERROR;
import static com.here.xyz.hub.util.health.schema.Status.Result.OK;
import static com.here.xyz.hub.util.health.schema.Status.Result.UNAVAILABLE;

import com.here.xyz.hub.util.health.schema.Response;
import com.here.xyz.hub.util.health.schema.Status;
import java.util.function.BooleanSupplier;

public class FunctionCheck extends ExecutableCheck {

	BooleanSupplier checkImpl;
	
	public FunctionCheck(BooleanSupplier impl) {
		this.checkImpl = impl;
	}
	
	public FunctionCheck(BooleanSupplier impl, String name) {
		this(impl);
		this.setName(name);
	}
	
	public FunctionCheck(BooleanSupplier impl, String name, Target target, Role role) {
		this(impl, name);
		setTarget(target);
		setRole(role);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Status execute() {
		Status s = new Status();
		try {
			s.setResult(checkImpl.getAsBoolean() ? OK : UNAVAILABLE);
		}
		catch (Throwable t) {
			s.setResult(ERROR);
			Response r = new Response();
			r.setMessage(t.getMessage());
			setResponse(r);
		}
		return s;
	}

}
