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

package com.here.xyz.hub.rest;

import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.Map;

public class HttpException extends Exception {

	private static final long serialVersionUID = 3414027446220801809L;
	public final HttpResponseStatus status;
	public final Map<String, Object> errorDetails;

	public HttpException(HttpResponseStatus status, String errorText) {
		super(errorText);
		this.status = status;
		this.errorDetails = null;
	}

	public HttpException(HttpResponseStatus status, String errorText, Map<String, Object>  errorDetails){
		super(errorText);
		this.status = status;
		this.errorDetails = errorDetails;
	}

	public HttpException(HttpResponseStatus status, String errorText, Throwable cause) {
		super(errorText, cause);
		this.status = status;
		this.errorDetails = null;
	}
}
