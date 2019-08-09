/*
 * Copyright 2019 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kie.hacep;

import java.util.Optional;

import org.kie.hacep.util.PrinterLogImpl;
import org.kie.remote.CommonConfig;

public final class EnvConfig {

    private String namespace;
    private String eventsTopicName;
    private String controlTopicName;
    private String snapshotTopicName;
    private String kieSessionInfosTopicName;
    private String printerType;
    private int iterationBetweenSnapshot = Config.DEFAULT_ITERATION_BETWEEN_SNAPSHOT;
    private int pollTimeout = 1000;
    private int maxSnapshotRequestAttempts = 10;
    private boolean skipOnDemanSnapshot;
    private long maxSnapshotAge;
    private boolean test;
    private boolean local;

    private EnvConfig() {
    }

    public static EnvConfig getDefaultEnvConfig() {
        return anEnvConfig().
                withNamespace(Optional.ofNullable(System.getenv(Config.NAMESPACE)).orElse(CommonConfig.DEFAULT_NAMESPACE)).
                withControlTopicName(Optional.ofNullable(System.getenv(Config.DEFAULT_CONTROL_TOPIC)).orElse(Config.DEFAULT_CONTROL_TOPIC)).
                withEventsTopicName(Optional.ofNullable(System.getenv(CommonConfig.DEFAULT_EVENTS_TOPIC)).orElse(CommonConfig.DEFAULT_EVENTS_TOPIC)).
                withSnapshotTopicName(Optional.ofNullable(System.getenv(Config.DEFAULT_SNAPSHOT_TOPIC)).orElse(Config.DEFAULT_SNAPSHOT_TOPIC)).
                withKieSessionInfosTopicName(Optional.ofNullable(System.getenv(CommonConfig.DEFAULT_KIE_SESSION_INFOS_TOPIC)).orElse(CommonConfig.DEFAULT_KIE_SESSION_INFOS_TOPIC)).
                withPrinterType(Optional.ofNullable(System.getenv(Config.DEFAULT_PRINTER_TYPE)).orElse(PrinterLogImpl.class.getName())).
                withPollTimeout(Optional.ofNullable(System.getenv(Config.POLL_TIMEOUT_MS)).orElse(String.valueOf(Config.DEFAULT_POLL_TIMEOUT_MS))).
                skipOnDemandSnapshot(Optional.ofNullable(System.getenv(Config.SKIP_ON_DEMAND_SNAPSHOT)).orElse(Boolean.FALSE.toString())).
                withIterationBetweenSnapshot(Optional.ofNullable(System.getenv(Config.ITERATION_BETWEEN_SNAPSHOT)).orElse(String.valueOf(Config.DEFAULT_ITERATION_BETWEEN_SNAPSHOT))).
                withMaxSnapshotAgeSeconds(Optional.ofNullable(System.getenv(Config.MAX_SNAPSHOT_AGE)).orElse(Config.DEFAULT_MAX_SNAPSHOT_AGE_SEC)).
                withMaxSnapshotRequestAttempts(Optional.ofNullable(System.getenv(Config.MAX_SNAPSHOT_REQUEST_ATTEMPTS)).orElse(Config.DEFAULT_MAX_SNAPSHOT_REQUEST_ATTEMPTS)).
                underTest(Optional.ofNullable(System.getenv(Config.UNDER_TEST)).orElse(Config.TEST));
    }

    public static EnvConfig anEnvConfig() {
        return new EnvConfig();
    }

    public EnvConfig withNamespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    public EnvConfig withEventsTopicName(String eventsTopicName) {
        this.eventsTopicName = eventsTopicName;
        return this;
    }

    public EnvConfig withControlTopicName(String controlTopicName) {
        this.controlTopicName = controlTopicName;
        return this;
    }

    public EnvConfig withSnapshotTopicName(String snapshotTopicName) {
        this.snapshotTopicName = snapshotTopicName;
        return this;
    }

    public EnvConfig withKieSessionInfosTopicName(String kieSessionInfosTopicName) {
        this.kieSessionInfosTopicName = kieSessionInfosTopicName;
        return this;
    }

    public EnvConfig withPrinterType(String printerType) {
        this.printerType = printerType;
        return this;
    }

    public EnvConfig withPollTimeout(String pollTimeout) {
        this.pollTimeout = Integer.valueOf(pollTimeout);
        return this;
    }

    public EnvConfig withIterationBetweenSnapshot(String iterationBetweenSnapshot) {
        this.iterationBetweenSnapshot = Integer.valueOf(iterationBetweenSnapshot);
        return this;
    }

    public EnvConfig underTest(String underTest) {
        return underTest(Boolean.valueOf(underTest));
    }

    public EnvConfig underTest(boolean underTest) {
        this.test = underTest;
        return this;
    }

    public EnvConfig local(boolean local) {
        this.local = local;
        return this;
    }

    public EnvConfig skipOnDemandSnapshot(String skipOnDemandSnapshoot) {
        this.skipOnDemanSnapshot = Boolean.valueOf(skipOnDemandSnapshoot);
        return this;
    }

    public EnvConfig withMaxSnapshotAgeSeconds(String maxSnapshotAge) {
        this.maxSnapshotAge = Long.valueOf(maxSnapshotAge);
        return this;
    }

    public EnvConfig withMaxSnapshotRequestAttempts(String maxSnapshotRequestAttempts) {
        this.maxSnapshotRequestAttempts = Integer.parseInt(maxSnapshotRequestAttempts);
        return this;
    }

    public EnvConfig clone() {
        EnvConfig envConfig = new EnvConfig();
        envConfig.eventsTopicName = this.eventsTopicName;
        envConfig.namespace = this.namespace;
        envConfig.controlTopicName = this.controlTopicName;
        envConfig.snapshotTopicName = this.snapshotTopicName;
        envConfig.kieSessionInfosTopicName = this.kieSessionInfosTopicName;
        envConfig.printerType = this.printerType;
        envConfig.test = this.test;
        envConfig.local = this.local;
        envConfig.pollTimeout = this.pollTimeout;
        envConfig.iterationBetweenSnapshot = this.iterationBetweenSnapshot;
        envConfig.skipOnDemanSnapshot = this.skipOnDemanSnapshot;
        envConfig.maxSnapshotAge = this.maxSnapshotAge;
        envConfig.maxSnapshotRequestAttempts = this.maxSnapshotRequestAttempts;
        return envConfig;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getEventsTopicName() {
        return eventsTopicName;
    }

    public String getControlTopicName() {
        return controlTopicName;
    }

    public String getSnapshotTopicName() {
        return snapshotTopicName;
    }

    public String getKieSessionInfosTopicName() {
        return kieSessionInfosTopicName;
    }

    public String getPrinterType() {
        return printerType;
    }

    public boolean isUnderTest() {
        return test;
    }

    public int getPollTimeout() {
        return pollTimeout;
    }

    public int getIterationBetweenSnapshot() {
        return iterationBetweenSnapshot;
    }

    public boolean isSkipOnDemanSnapshot() {
        return skipOnDemanSnapshot;
    }

    public long getMaxSnapshotAge() {
        return maxSnapshotAge;
    }

    public boolean isLocal() {
        return local;
    }

    public int getMaxSnapshotRequestAttempts() {
        return maxSnapshotRequestAttempts;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("EnvConfig{");
        sb.append("namespace='").append(namespace).append('\'');
        sb.append(", eventsTopicName='").append(eventsTopicName).append('\'');
        sb.append(", controlTopicName='").append(controlTopicName).append('\'');
        sb.append(", snapshotTopicName='").append(snapshotTopicName).append('\'');
        sb.append(", kieSessionInfosTopicName='").append(kieSessionInfosTopicName).append('\'');
        sb.append(", printerType='").append(printerType).append('\'');
        sb.append(", pollTimeout='").append(pollTimeout).append('\'');
        sb.append(", iterationBetweenSnapshot='").append(iterationBetweenSnapshot).append('\'');
        sb.append(", skipOnDemanSnapshot='").append(skipOnDemanSnapshot).append('\'');
        sb.append(", maxSnapshotAge='").append(maxSnapshotAge).append('\'');
        sb.append(", maxSnapshotRequestAttempts='").append(maxSnapshotRequestAttempts).append('\'');
        sb.append(", underTest='").append(test).append('\'');
        sb.append('}');
        return sb.toString();
    }
}

