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

package com.here.xyz.httpconnector.task;

import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.config.JDBCMaintainer;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.psql.DatabaseMaintainer;
import com.here.xyz.responses.maintenance.ConnectorStatus;
import com.here.xyz.responses.maintenance.SpaceStatus;
import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;

public class MaintenanceHandler {


    private static final Logger logger = LogManager.getLogger();

    public static Future<ConnectorStatus> getConnectorStatus(String connectorId) {
        return JDBCMaintainer.getConnectorStatus(connectorId);
    }

    public static Future<SpaceStatus> getMaintenanceStatusOfSpace(String spaceId) {
        return JDBCMaintainer.getMaintenanceStatusOfSpace(spaceId);
    }

    public static Future<ConnectorStatus> initializeOrUpdateDatabase(String connectorId, boolean force) {
        if(force){
            logger.info("Start database initialization for connector: {} ", connectorId);
            return JDBCMaintainer.initializeOrUpdateDatabase(connectorId);
        }else {
            return getConnectorStatus(connectorId)
                    .compose(connectorStatus -> {
                        if (connectorStatus != null && connectorStatus.isInitialized()) {
                            /** Nothing more to do */
                            logger.info("Database is already initialized for connector: {}", connectorId);
                            return Future.succeededFuture(connectorStatus);
                        } else {
                            logger.info("Start database initialization for connector: {} ", connectorId);
                            return JDBCMaintainer.initializeOrUpdateDatabase(connectorId);
                        }
                    });
        }
    }

    public static Future<Void> maintainSpace(String spaceId, boolean force) {
        if (!force) {
            return getMaintenanceStatusOfSpace(spaceId)
                    .compose(spaceStatus -> {
                        if (spaceStatus != null && spaceStatus.isIdxCreationFinished() != null && !spaceStatus.isIdxCreationFinished())
                            return Future.failedFuture(new HttpException(CONFLICT, "Index creation is currently running!"));
                        return JDBCMaintainer.maintainSpace(spaceId);
                    });
        }
        return JDBCMaintainer.maintainSpace(spaceId);
    }

    public static Future<Void> purgeOldVersions(String spaceId, Long minTagVersion) {
        return JDBCMaintainer.purgeOldVersions(spaceId, minTagVersion);
    }

    public static Future<Void> maintainIndices(String connectorId, boolean autoIndexing) {

        return getConnectorStatus(connectorId)
                .compose(connectorStatus -> {
                    /** Check if DB is initialized - if yes check script versions */

                    if (connectorStatus != null && connectorStatus.isInitialized()) {
                        if (DatabaseMaintainer.XYZ_EXT_VERSION > connectorStatus.getScriptVersions().get("ext")
                                || DatabaseMaintainer.H3_CORE_VERSION > connectorStatus.getScriptVersions().get("h3")) {
                            logger.info("Database needs an update: {}", connectorId);
                            return JDBCMaintainer.initializeOrUpdateDatabase(connectorId)
                                    .map(f -> connectorStatus);
                        }
                        return Future.succeededFuture(connectorStatus);
                    } else {
                        logger.warn("Database not initialized for connector: {}", connectorId);
                        return Future.failedFuture(new HttpException(METHOD_NOT_ALLOWED, "Database not initialized!"));
                    }
                }).compose(connectorStatus -> {
                    /** Check MaintenanceStatus of Connector and decide if force run is needed */

                    boolean force = false;

                    if (connectorStatus.getMaintenanceStatus() != null
                            && connectorStatus.getMaintenanceStatus().get(ConnectorStatus.AUTO_INDEXING) != null) {

                        ConnectorStatus.MaintenanceStatus autoIndexingStatus = connectorStatus.getMaintenanceStatus().get(ConnectorStatus.AUTO_INDEXING);

                        if (autoIndexingStatus.getMaintenanceRunning() != null && autoIndexingStatus.getMaintenanceRunning().size() > 0) {
                            Long timeSinceLastRunInHr = (Core.currentTimeMillis() - autoIndexingStatus.getMaintainedAt()) / 1000 / 60 / 60;
                            if (timeSinceLastRunInHr > CService.configuration.MISSING_MAINTENANCE_WARNING_IN_HR) {
                                logger.warn("Last MaintenanceRun is older than {}h - connector: {}", timeSinceLastRunInHr, connectorId);
                                force = true;
                            }else  if (autoIndexingStatus.getMaintenanceRunning().size() >= CService.configuration.MAX_CONCURRENT_MAINTENANCE_TASKS) {
                                return Future.failedFuture(new HttpException(CONFLICT, "Maximal concurrent Indexing tasks are running!"));
                            }
                        }
                    }
                    logger.info("Start maintain indices for connector: {}", connectorId);
                    return JDBCMaintainer.maintainIndices(connectorId, autoIndexing, force);
                });
    }
}