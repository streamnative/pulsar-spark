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
package org.apache.pulsar.tests.common.framework.invokers.docker;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.tests.common.framework.FrameworkUtils;
import org.testcontainers.containers.GenericContainer;

/**
 * Container used for docker testing.
 */
@Slf4j
public class DockerTestContainer extends GenericContainer<DockerTestContainer> {

    private static final String IMAGE = "java:8";

    private final String className;
    private final String simpleClassName;
    private final String methodName;
    private final String dockertestJarPath;
    private final String containerName;

    public DockerTestContainer(String className,
                               String methodName,
                               String dockertestJarPath) {
        super(IMAGE);
        this.className = className;
        String[] classNameParts = StringUtils.split(className, '.');
        this.simpleClassName = classNameParts[classNameParts.length - 1];
        this.methodName = methodName;
        this.dockertestJarPath = dockertestJarPath;
        this.containerName = simpleClassName + "-" + methodName + "-" + FrameworkUtils.randomName(8);
    }

    @Override
    public String getContainerName() {
        return containerName;
    }

    @Override
    public void start() {
        Map<String, String> labels = new HashMap<>(2);
        labels.put("testMethodName", methodName);
        labels.put("testClassName", className);
        this.withCreateContainerCmdModifier(createContainerCmd -> {
            createContainerCmd.withName(containerName);
            createContainerCmd.withLabels(labels);
            createContainerCmd.withWorkingDir("/");
            createContainerCmd.withCmd(
                "java",
                "-DsystemTestInvoker=LOCAL",
                "-cp", "/dockertest.jar",
                "org.apache.pulsar.tests.common.framework.SystemTestRunner",
                className + "#" + methodName
            );
        });

        super.start();
        log.info("Start dockerTest container at {}", getContainerName());
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DockerTestContainer)) {
            return false;
        }

        DockerTestContainer another = (DockerTestContainer) o;
        return containerName.equals(another.containerName)
            && super.equals(another);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(containerName);
    }


}
