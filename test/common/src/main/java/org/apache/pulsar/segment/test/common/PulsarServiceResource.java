/**
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
 */
package org.apache.pulsar.segment.test.common;

import com.google.common.collect.Sets;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import org.apache.bookkeeper.test.PortManager;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.junit.rules.ExternalResource;

/**
 * Implement a junit external resources that starts a pulsar service.
 */
public class PulsarServiceResource extends ExternalResource {

    Path testDataPath;
    LocalBookkeeperEnsemble localEnsemble;
    PulsarService pulsarService;
    int zkPort;
    int brokerServicePort;
    int webServicePort;

    @Override
    protected void before() throws Throwable {
        super.before();

        testDataPath = Files.createTempDirectory("pulsar");

        Path zkDirPath = Files.createDirectory(Paths.get(testDataPath.toString(), "zk"));
        Path bkDirPath = Files.createDirectory(Paths.get(testDataPath.toString(), "bk"));

        zkPort = PortManager.nextFreePort();

        localEnsemble = new LocalBookkeeperEnsemble(
            3,
            zkPort,
            0,
            zkDirPath.toString(),
            bkDirPath.toString(),
            true,
            "127.0.0.1",
            () -> PortManager.nextFreePort());
        localEnsemble.start();

        brokerServicePort = PortManager.nextFreePort();
        webServicePort = PortManager.nextFreePort();

        ServiceConfiguration serviceConf = new ServiceConfiguration();
        serviceConf.setBrokerServicePort(brokerServicePort);
        serviceConf.setWebServicePort(webServicePort);
        serviceConf.setAdvertisedAddress("127.0.0.1");
        serviceConf.setClusterName("standalone");
        serviceConf.setZookeeperServers("127.0.0.1:" + zkPort);
        serviceConf.setConfigurationStoreServers("127.0.0.1:" + zkPort);
        serviceConf.setRunningStandalone(true);

        pulsarService = new PulsarService(serviceConf, Optional.empty());
        pulsarService.start();

        try (PulsarAdmin admin = PulsarAdmin.builder()
             .serviceHttpUrl(getWebServiceUrl())
             .build()) {
            createDefaultNameSpace(admin, serviceConf, serviceConf.getClusterName());
        }
    }

    private void createDefaultNameSpace(PulsarAdmin admin,
                                        ServiceConfiguration config,
                                        String cluster) throws Exception {
        ClusterData clusterData = new ClusterData(
            getWebServiceUrl(), null /* serviceUrlTls */,
            getBrokerServiceUrl(), null /* brokerServiceUrlTls */);
        if (!admin.clusters().getClusters().contains(cluster)) {
            admin.clusters().createCluster(cluster, clusterData);
        } else {
            admin.clusters().updateCluster(cluster, clusterData);
        }

        // Create a public tenant and default namespace
        final String publicTenant = TopicName.PUBLIC_TENANT;
        final String defaultNamespace = TopicName.PUBLIC_TENANT + "/" + TopicName.DEFAULT_NAMESPACE;
        if (!admin.tenants().getTenants().contains(publicTenant)) {
            admin.tenants().createTenant(publicTenant,
                    new TenantInfo(Sets.newHashSet(config.getSuperUserRoles()), Sets.newHashSet(cluster)));
        }
        if (!admin.namespaces().getNamespaces(publicTenant).contains(defaultNamespace)) {
            admin.namespaces().createNamespace(defaultNamespace);
            admin.namespaces().setNamespaceReplicationClusters(
                defaultNamespace, Sets.newHashSet(config.getClusterName()));
        }
    }

    @Override
    protected void after() {
        try {
            if (null != pulsarService) {
                pulsarService.close();
            }
            if (null != localEnsemble) {
                localEnsemble.stop();
            }
            MoreFiles.deleteRecursively(testDataPath, RecursiveDeleteOption.ALLOW_INSECURE);
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete test data `" + testDataPath + "`", e);
        }
        super.after();
    }

    public String getZkServers() {
        return "127.0.0.1:" + zkPort;
    }

    public String getBrokerServiceUrl() {
        return "pulsar://127.0.0.1:" + brokerServicePort;
    }

    public String getWebServiceUrl() {
        return "http://127.0.0.1:" + webServicePort;
    }
}
