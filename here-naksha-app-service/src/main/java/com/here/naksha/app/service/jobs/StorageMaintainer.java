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
package com.here.naksha.app.service.jobs;

import com.here.naksha.app.service.NakshaHubConfig;
import com.here.naksha.app.service.models.MaintenanceTrigger;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.core.storage.ITransactionSettings;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background job thread, responsible for invoking maintenance process on all applicable Naksha storages
 * (AdminDB and SpaceDBs) at pre-configured frequency.
 */
public class StorageMaintainer {

  private static final Logger logger = LoggerFactory.getLogger(StorageMaintainer.class);

  /**
   * NakshaHub configuration to be used by this job (e.g. execution frequency)
   */
  private final @NotNull NakshaHubConfig config;
  /**
   * NakshaHub configuration to be used by this job (e.g. execution frequency)
   */
  private final @NotNull IStorage adminStorage;
  /**
   * Transaction Settings to be used while creating new DB connection
   */
  private @NotNull ITransactionSettings txSettings;

  /**
   * Create a new Storage Maintainer instance, which once initiated, will run as a background job
   * to invoke maintenance on all Naksha associated storages (adminDB and spaceDBs).
   * Ideally should be instantiated only once per Naksha instance.
   *
   * @param config        The NakshaHub configuration to read desired job frequency/interval
   * @param adminStorage  The Naksha AdminDB Storage implementation class, used to fetch collection details
   * @param txSettings    The transaction settings to be used on creating DB connection running maintenance.
   */
  public StorageMaintainer(
      final @NotNull NakshaHubConfig config,
      final @NotNull IStorage adminStorage,
      final @NotNull ITransactionSettings txSettings) {
    this.config = config;
    this.adminStorage = adminStorage;
    this.txSettings = txSettings;
  }

  /**
   * The only method, which should be invoked for setting up the job schedule at preconfigured
   * delay and interval. Ideally, this should be invoked while starting Naksha service.
   * This should be invoked only once per Naksha instance.
   * Internally, it will handle the multi-instance concurrency.
   */
  public void initiate() {
    final int initialDelay = config.maintenanceInitialDelayInMins;
    final int subsequentDelay = config.maintenanceIntervalInMins;
    // Schedule maintenance job as per configured frequency (e.g. 12 hours)
    new ScheduledThreadPoolExecutor(1)
        .scheduleWithFixedDelay(this::startJob, initialDelay, subsequentDelay, TimeUnit.MINUTES);
    logger.info(
        "Storage Maintenance job is set to start after {} mins with subsequent delay of {} mins.",
        initialDelay,
        subsequentDelay);
  }

  /**
   * This method is internally invoked at every scheduled execution of a job.
   * It prepared list of all spaces across spaceDBs and adminDB for which maintenace to be triggered.
   * It then distributes the list for processing using a thread pool.
   * It internally ensures multi-instance lock to ensure only one instance of job is running
   * (except for an erroneous situation where previously submitted job is stuck for too long and
   * we have to still go ahead and trigger a new one)
   */
  protected void startJob() {
    Thread.currentThread().setName("maintenance");
    logger.info("Storage Maintenance job got triggered.");
    // TODO : Acquire lock at job level in adminDB (to ensure only 1 job runs across instances)
    // TODO : Fetch all Spaces
    // TODO : Fetch all Connectors
    // TODO : Fetch all Storages
    /* TODO : Convert all spaces into Map of <triggerKey, MaintenanceTriggerEvent>, but only for those connectors
    that have associated storages.
    TriggerKey is (likely the connectorId) to consolidate maintenance of all related resources under one thread.
    We will also include adminDB collections as part of maintenance job (apart from spaceDB collections). */
    // TODO : distribute all Maintenance Trigger events to a thread pool for processing
    // TODO : wait for all jobs to complete processing (with max timeout)
    // TODO : Release lock at job level
  }

  /**
   * This method is internally invoked in parallel against individual spaceDBs.
   * It instantiates appropriate storage implementation class and triggers maintenance() method
   * by passing list of collections to run maintenance for.
   *
   * @param trigger       The Maintenance Trigger details like target storage, spaces
   */
  protected void triggerMaintenance(final @NotNull MaintenanceTrigger trigger) {
    Thread.currentThread().setName("maintenance-" + trigger.key());
    // TODO : Acquire lock at TriggerKey level in adminDB (to ensure only 1 trigger runs across instances)
    // TODO : Prepare list of collections (to run maintenance for)
    // TODO : Invoke maintenance() using appropriate storage implementation class, by passing list of collections
    // TODO HP_QUERY : How do we mention where is naksha_tx table, while triggering maintenance?
    // TODO : Release lock at TriggerKey level
  }
}
