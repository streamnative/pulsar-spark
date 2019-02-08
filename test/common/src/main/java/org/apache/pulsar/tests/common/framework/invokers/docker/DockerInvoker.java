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

import com.github.dockerjava.api.command.InspectContainerResponse.ContainerState;
import com.github.dockerjava.api.exception.DockerException;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.pulsar.tests.common.framework.FrameworkUtils;
import org.apache.pulsar.tests.common.framework.ServiceContext;
import org.apache.pulsar.tests.common.framework.TestInvoker;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;

/**
 * A invoker implementation that invokes junit test in a docker image.
 */
@Slf4j
public class DockerInvoker implements TestInvoker {

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);
    private DockerTestContainer container;

    @Override
    public CompletableFuture<Void> invokeAsync(Method testMethod, ServiceContext serviceCtx) {
        log.info("Starting invoke method : {}", testMethod);

        String className = testMethod.getDeclaringClass().getName();
        String methodName = testMethod.getName();

        return CompletableFuture
            .runAsync(() -> invokeTests(className, methodName, serviceCtx))
            .thenCompose(ignored1 -> waitForJobCompletion())
            .<Void>thenApply(state -> {
                if (state.getExitCode() != 0) {
                    throw new AssertionError("Test failed "
                        + className + "#" + methodName);
                }
                executorService.shutdown();
                return null;
            })
            .whenComplete((v, ex) -> {
                if (null != ex) {
                    log.error("Error while invoking test {}#{}", className, methodName);
                }
            });
    }

    private void invokeTests(String className,
                             String methodName,
                             ServiceContext serviceContext) throws DockerException {
        log.info("Invoking test {}#{}", className, methodName);

        String dockertestJarPath = FrameworkUtils.getDockerTestJarPath();
        log.info("DockerTest Jar Path: {}", dockertestJarPath);

        Network network = null;
        if (serviceContext instanceof DockerServiceContext) {
            DockerServiceContext dsc = (DockerServiceContext) serviceContext;
            network = dsc.getNetwork();
        }
        if (network == null) {
            network = Network.newNetwork();
        }

        // create the container
        this.container = new DockerTestContainer(
                className,
                methodName,
                dockertestJarPath
            )
            .withLogConsumer(new Slf4jLogConsumer(log))
            .withNetwork(network);
        this.container.start();
    }

    private ContainerState getContainerState() {
        return this.container.getDockerClient()
            .inspectContainerCmd(this.container.getContainerId())
            .exec()
            .getState();
    }

    private CompletableFuture<ContainerState> waitForJobCompletion() {
        CompletableFuture<ContainerState> doneFuture = new CompletableFuture<>();
        final AtomicReference<ScheduledFuture<?>> checkTaskRef = new AtomicReference<>();
        checkTaskRef.set(executorService.scheduleAtFixedRate(() -> {
            ContainerState state = getContainerState();
            log.info("Check docker container state : {}", state);
            if (!state.getRunning()) {
                // the test is not running any more
                ScheduledFuture<?> checkTask = checkTaskRef.get();
                if (null != checkTask) {
                    checkTask.cancel(false);
                }
                FutureUtils.complete(doneFuture, state);
            }
        }, 3, 3, TimeUnit.SECONDS));
        return doneFuture;
    }

    @Override
    public void stop() {
        if (null != container) {
            container.stop();
        }
    }
}
