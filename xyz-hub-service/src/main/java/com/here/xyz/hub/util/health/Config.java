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
package com.here.xyz.hub.util.health;

import com.here.xyz.hub.Service;
import java.util.HashMap;
import java.util.Map;

public class Config {

	private static final String ENV_VAR_PREFIX = "CICD_HEALTH_";
	public static final String DEFAULT_HEALTH_CHECK_HEADER_NAME = "health-check";
	public static final String DEFAULT_HEALTH_CHECK_HEADER_VALUE = "true";

	public static String getEnvName(Setting s) {
		return ENV_VAR_PREFIX + s;
	}
	
	public enum Setting {
		BOOT_GRACE_TIME,
		CHECK_DEFAULT_INTERVAL,
		CHECK_DEFAULT_TIMEOUT
	}
	
	@SuppressWarnings("serial")
	private static final Map<Setting, Object> defaults = new HashMap<Setting, Object>() {{
		put(Setting.CHECK_DEFAULT_INTERVAL, 30000);
		put(Setting.CHECK_DEFAULT_TIMEOUT, 10000);
		put(Setting.BOOT_GRACE_TIME, 0);
	}};
	
	private static Object gatherSetting(Setting key) {
		String value = System.getenv(ENV_VAR_PREFIX + key);
		if (value != null) return value;
		return defaults.get(key);
	}
	
	public static int getInt(Setting key) {
		Object value = gatherSetting(key);
		if (value instanceof String) return Integer.parseInt((String) value);
		return ((Number) value).intValue();
	}
	
	public static String getString(Setting key) {
		Object value = gatherSetting(key);
		return value.toString();
	}
	
	@SuppressWarnings("unused")
	public static boolean getBoolean(Setting key) {
		Object value = gatherSetting(key);
		if (value instanceof String) return Boolean.parseBoolean((String) value);
		return (boolean) value;
	}

	public static String getHealthCheckHeaderName(){
		if(Service.configuration.HEALTH_CHECK_HEADER_NAME != null )
			return Service.configuration.HEALTH_CHECK_HEADER_NAME;
		return DEFAULT_HEALTH_CHECK_HEADER_NAME;
	}

	public static String getHealthCheckHeaderValue(){
		if(Service.configuration.HEALTH_CHECK_HEADER_VALUE != null )
			return Service.configuration.HEALTH_CHECK_HEADER_VALUE;
		return DEFAULT_HEALTH_CHECK_HEADER_VALUE;
	}
}
