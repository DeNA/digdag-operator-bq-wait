/*
* SPDX-FileCopyrightText: 2020 DeNA Co., Ltd.
* SPDX-License-Identifier: Apache-2.0
*
* Copyright (c) 2020 DeNA Co., Ltd.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*        http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.dena.digdag.plugin.gcp

import com.google.cloud.bigquery.BigQueryException
import com.google.cloud.bigquery.BigQueryOptions
import com.google.common.base.Optional
import io.digdag.core.workflow.OperatorTestingUtils
import io.digdag.spi.ImmutableTaskRequest
import io.digdag.spi.SecretProvider
import io.digdag.spi.TaskExecutionException
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.time.ZoneId
import spock.lang.Specification

import static io.digdag.client.config.ConfigUtils.newConfig

import com.google.cloud.bigquery.TableId

/**
 * This spec suppose...
 * - You've got valid GCP project
 * - Application default credential is set up on your machine
 * - There is dataset named 'test_bq_wait' in your BQ.
 * - There are/aren't some tables in test_bq_wait.
 * 
 * Some features expect certain env val is defined.
 */

// TODO Automate seting up test tables.
class BqWaitOperatorSpec extends Specification {
    def opFactory = new BqWaitOperatorFactory()
    // set project path as current directory.
    def pjPath = Paths.get(System.getProperty("user.dir"))
    def bigquery = BigQueryOptions.getDefaultInstance().getService()

    def "table found"() {
        when:
        def result = runTask(Instant.now(), "test_bq_wait.table_found")

        then:
        result != null
    }

    def "table not found"() {
        when:
        def result = runTask(Instant.now(), "test_bq_wait.table_not_found")

        then:
        // pollingWaiter.await throws TaskExecutionException to keep polling.
        TaskExecutionException e = thrown()
        !e.isError()
    }

    def "project id specified"() {
        when:
        def gcp_pj = System.getenv("gcp_pj")
        if(gcp_pj == null)
            throw new RuntimeException("You must assign GCP project id to env var 'gcp_pj' to run this spec.")

        def result = runTask(Instant.now(), gcp_pj + ".test_bq_wait.table_found")

        then:
        result != null
    }

    def "partition found"() {
        when:
        def result = runTask(Instant.now(), "test_bq_wait.partition\$20200101")

        then:
        result != null
    }

    def "partition not found"() {
        when:
        def result = runTask(Instant.now(), "test_bq_wait.partition\$20200103")

        then:
        TaskExecutionException e = thrown()
        !e.isError()
    }

    def "monthly partition found"() {
        when:
        def result = runTask(Instant.now(), "test_bq_wait.partition_monthly\$202001")

        then:
        result != null
    }

    def "hourly partition found"() {
        when:
        def result = runTask(Instant.now(), "test_bq_wait.partition_hourly\$2020010100")

        then:
        result != null
    }

    def "yearly partition found"() {
        when:
        def result = runTask(Instant.now(), "test_bq_wait.partition_yearly\$2020")

        then:
        result != null
    }

    def "table updated after session time"() {
        when:
        def table_name =  "test_bq_wait.partition"
        def lastModifiedTime = getLastModifiedTime(table_name)
        def sessionTime = lastModifiedTime.minus(1, ChronoUnit.HOURS)
        def result = runTask(sessionTime,table_name, "PT1H")

        then:
        result != null
    }

    def "table not updated"() {
        when:
        def table_name =  "test_bq_wait.partition"
        def lastModifiedTime = getLastModifiedTime(table_name)
        def sessionTime = lastModifiedTime.minus(1, ChronoUnit.HOURS).plus(1, ChronoUnit.SECONDS)
        def result = runTask(sessionTime,table_name, "PT1H")

        then:
        TaskExecutionException e = thrown()
        !e.isError()
    }

    def "table updated before session time"() {
        when:
        def table_name =  "test_bq_wait.partition"
        def lastModifiedTime = getLastModifiedTime(table_name)
        def sessionTime = lastModifiedTime.plus(1, ChronoUnit.HOURS)
        def result = runTask(sessionTime,table_name, "-PT1H")

        then:
        result != null
    }

    def "use gcp.credential"() {
        when:
        // This feature suppose you have created service account without any permission
        // and you have set json key in the env val named "gcp_credential"
        def gcpCredential = System.getenv("gcp_credential")
        if(gcpCredential == null)
            throw new RuntimeException("You must assign service account json key to env var 'gcp_credential' to run this spec.")

        def mockSecretProvider = Stub(SecretProvider) {
           getSecretOptional("gcp.credential") >> { Optional.of(gcpCredential) }
        }
        def result = runTask(Instant.now(), "test_bq_wait.table_found", mockSecretProvider)

        then:
        TaskExecutionException ex = thrown()
        BigQueryException e = ex.getCause()
        e.getCode() == 403 && e.getReason().equals("accessDenied")
    }

    /*
    *  Helper methods.
    */
    def getLastModifiedTime(String table_name) {
        def (dataset, table) = table_name.split("\\.")
        Instant.ofEpochMilli(bigquery.getTable(dataset, table).getLastModifiedTime())
    }

    def runTask(Instant sessionTime, String table_spec) {
        runTask(sessionTime, table_spec, "") 
    }

    def runTask(Instant sessionTime, String table_spec, SecretProvider secrets) {
        def context = OperatorTestingUtils.newContext(pjPath, newTaskRequest(sessionTime, table_spec, ""), secrets)
        def op = opFactory.newOperator(context)
        op.runTask()
    }

    def runTask(Instant sessionTime, String table_spec, String updated_after) {
        def context = OperatorTestingUtils.newContext(pjPath, newTaskRequest(sessionTime, table_spec, updated_after))
        def op = opFactory.newOperator(context)
        op.runTask()
    }

    def newTaskRequest(Instant sessionTime, String command, String updated_after) {
        def param = newConfig()
        param.set("_command", command)
        if(updated_after != null && !updated_after.isEmpty())
            param.set("updated_after", updated_after)

        ImmutableTaskRequest.builder()
            .siteId(1)
            .projectId(2)
            .workflowName("wf")
            .revision(Optional.of("rev"))
            .taskId(3)
            .attemptId(4)
            .sessionId(5)
            .taskName("t")
            .lockId("l")
            .timeZone(ZoneId.systemDefault())
            .sessionUuid(UUID.randomUUID())
            .sessionTime(sessionTime)
            .createdAt(Instant.now())
            .config(param)
            .localConfig(newConfig())
            .lastStateParams(newConfig())
            .isCancelRequested(false)
            .build()
    }
}
