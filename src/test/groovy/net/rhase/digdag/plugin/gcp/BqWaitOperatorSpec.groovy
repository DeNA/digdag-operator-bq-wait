package net.rhase.digdag.plugin.gcp

import com.google.cloud.bigquery.BigQueryOptions
import com.google.common.base.Optional
import io.digdag.spi.ImmutableTaskRequest
import io.digdag.spi.TaskExecutionException
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.time.ZoneId
import spock.lang.Specification

import static io.digdag.client.config.ConfigUtils.newConfig
import static io.digdag.core.workflow.OperatorTestingUtils.newContext

import com.google.cloud.bigquery.TableId

/**
 * This spec suppose...
 * - You've got valid GCP project
 * - Application default credential is set up on your machine
 * - There is dataset named 'test_bq_wait' in your BQ.
 * - There are/aren't some tables in test_bq_wait.
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

    def "table updated"() {
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

    def getLastModifiedTime(String table_name) {
        def (dataset, table) = table_name.split("\\.")
        Instant.ofEpochMilli(bigquery.getTable(dataset, table).getLastModifiedTime())
    }

    def runTask(Instant sessionTime, String table_spec) {
        runTask(sessionTime, table_spec, null) 
    }

    def runTask(Instant sessionTime, String table_spec, String updated_after) {
        def context = newContext(pjPath, newTaskRequest(sessionTime, table_spec, updated_after))
        def op = opFactory.newOperator(context)
        op.runTask()
    }

    def newTaskRequest(Instant sessionTime, String command, String updated_after) {
        def param = newConfig()
        param.set("_command", command)
        if(updated_after != null)
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
            .build()
    }
}
