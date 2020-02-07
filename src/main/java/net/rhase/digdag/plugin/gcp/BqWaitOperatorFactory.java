/* Licensed under Apache-2.0 */
package net.rhase.digdag.plugin.gcp;

import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.BigQueryOptions.Builder;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigKey;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskResult;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.util.BaseOperator;

import java.time.Duration;
import java.time.format.DateTimeParseException;

import static io.digdag.standards.operator.state.PollingRetryExecutor.pollingRetryExecutor;
import static io.digdag.standards.operator.state.PollingWaiter.pollingWaiter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

public class BqWaitOperatorFactory implements OperatorFactory {

    @Override
    public String getType() {
        return "bq_wait";
    }

    @Override
    public Operator newOperator(OperatorContext context) {
        return new BqWaitOperator(context);
    }

    private static class BqWaitOperator extends BaseOperator {

        private final Config params;
        private final TaskState state;
        private final BigQuery bigquery;

        public BqWaitOperator(OperatorContext context) {
            super(context);
            this.params = request.getConfig();
            this.state = TaskState.of(request);
            Optional<String> gcpCredentialOpt = context.getSecrets().getSecretOptional("gcp.credential");

            Credentials credential;
            Builder bqBuilder = BigQueryOptions.newBuilder();
            bqBuilder.setHeaderProvider(FixedHeaderProvider.create("user-agent", "digdag"));

            if (gcpCredentialOpt.isPresent()) {
                try {
                    credential = ServiceAccountCredentials.fromStream(
                            new ByteArrayInputStream(gcpCredentialOpt.get().getBytes(StandardCharsets.UTF_8)));
                } catch (IOException e) {
                    throw new ConfigException("Invalid credential: " + gcpCredentialOpt.get(), e);
                }
            } else {
                try {
                    credential = ServiceAccountCredentials.getApplicationDefault();
                } catch (IOException e) {
                    throw new ConfigException("Could not get application default credential.", e);
                }
            }

            this.bigquery = bqBuilder.setCredentials(credential).build().getService();
        }

        @Override
        public TaskResult runTask() {
            Optional<String> cmdParam = params.getOptional("_command", String.class);
            Optional<String> updatedAfterParam = params.getOptional("updated_after", String.class);

            if (!cmdParam.isPresent())
                throw new ConfigException("No table specified.");

            String cmd = cmdParam.get();
            String[] cmdArray = cmd.split("\\.");
            TableId tableId;
            switch (cmdArray.length) {
            case 2:
                // [dataset_name].[table_name]
                tableId = TableId.of(cmdArray[0], cmdArray[1]);
                break;
            case 3:
                // {project_id}.[dataset_name].[table_name]
                tableId = TableId.of(cmdArray[0], cmdArray[1], cmdArray[2]);
                break;
            default:
                throw new ConfigException("Invalid table specification: " + cmd);
            }

            Duration updatedAfter = null;
            if (updatedAfterParam.isPresent()) {
                try {
                    // 'updated_after' must be in ISO-8601 duration format
                    // https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-
                    updatedAfter = Duration.parse(updatedAfterParam.get());
                } catch (DateTimeParseException e) {
                    throw new ConfigException("updated_after must be ISO-8601 duration.", e);
                }
            }

            Optional<Duration> updatedAfterOpt = Optional.fromNullable(updatedAfter);

            Table table = pollingWaiter(state, "exists").withWaitMessage("Target '%s' does not yet exist", cmd)
                    .await(pollState -> pollingRetryExecutor(pollState, "poll")
                            .retryIf(BigQueryException.class, BqWaitOperator::isRetryable)
                            .run(s -> checkTable(tableId)));

            Long lastModifiedTime;
            if (updatedAfterOpt.isPresent()) {
                lastModifiedTime = pollingWaiter(state, "updated").withWaitMessage("Target '%s' found but old", cmd)
                        .await(pollState -> pollingRetryExecutor(pollState, "poll_updated")
                                .retryIf(BigQueryException.class, BqWaitOperator::isRetryable)
                                .run(s -> checkUpdated(table, updatedAfterOpt.get())));
            } else {
                lastModifiedTime = table.getLastModifiedTime();
            }

            Config reqParams = request.getConfig().getFactory().create();
            Config resParams = reqParams.getNestedOrSetEmpty("bq_wait").getNestedOrSetEmpty("last_object");
            resParams.set("table", table.getFriendlyName());
            resParams.set("last_modified_time", lastModifiedTime);

            return TaskResult.defaultBuilder(request)
                    .resetStoreParams(ImmutableList.of(ConfigKey.of("bq_wait", "last_object"))).storeParams(resParams)
                    .build();
        }

        private Optional<Long> checkUpdated(Table table, Duration updatedAfter) {
            long mustBeNewerThan = request.getSessionTime().plus(updatedAfter).toEpochMilli();
            long lastModifiedTime = table.getLastModifiedTime();
            if (lastModifiedTime < mustBeNewerThan)
                return Optional.absent();

            return Optional.of(Long.valueOf(lastModifiedTime));
        }

        private static boolean isRetryable(BigQueryException e) {
            return e.isRetryable();
        }

        private Optional<Table> checkTable(TableId tableId) {
            Optional<Table> tbl = Optional.fromNullable(bigquery.getTable(tableId));
            // Table not found.
            if (!tbl.isPresent())
                return Optional.absent();

            // In case that table exists and partition does not exist,
            // Table object with zero rows will be returned.
            if (tableId.getTable().contains("$") && tbl.get().getNumRows().equals(BigInteger.ZERO))
                return Optional.absent();

            // Table found.
            return tbl;
        }
    }
}