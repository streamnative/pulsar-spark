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
package org.apache.pulsar.tests.common.container;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.InspectExecResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.StreamType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.utils.CharsetNames;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.pulsar.tests.common.framework.ExecResult;

/**
 * Docker utilities for integration tests.
 */
@Slf4j
public class DockerUtils {

    private static File getTargetDirectory(final String containerId) {
        String base = System.getProperty("maven.buildDirectory");
        if (base == null) {
            base = "target";
        }
        File directory = new File(base + "/container-logs/" + containerId);
        if (!directory.exists() && !directory.mkdirs()) {
            log.error("Error creating directory for container logs.");
        }
        return directory;
    }

    /**
     * Dump container log to target directory.
     *
     * @param dockerClient docker client to access the docker image.
     * @param containerId container id.
     */
    public static void dumpContainerLogToTarget(final DockerClient dockerClient,
                                                final String containerId) {
        final InspectContainerResponse inspectContainerResponse = dockerClient.inspectContainerCmd(containerId).exec();
        // docker api returns names prefixed with "/", it's part of it's legacy design,
        // this removes it to be consistent with what docker ps shows.
        final String containerName = inspectContainerResponse.getName().replace("/", "");
        File output = new File(getTargetDirectory(containerName), "docker.log");
        log.info("Dumping container logs for : {} to {}", containerName, output.getAbsolutePath());
        try (FileOutputStream os = new FileOutputStream(output)) {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            dockerClient.logContainerCmd(containerId).withStdOut(true)
                .withStdErr(true).withTimestamps(true).exec(new ResultCallback<Frame>() {
                        @Override
                        public void close() {}

                        @Override
                        public void onStart(Closeable closeable) {}

                        @Override
                        public void onNext(Frame object) {
                            try {
                                os.write(object.getPayload());
                            } catch (IOException e) {
                                onError(e);
                            }
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            future.completeExceptionally(throwable);
                        }

                        @Override
                        public void onComplete() {
                            future.complete(true);
                        }
                    });
            future.get();
        } catch (RuntimeException | ExecutionException | IOException e) {
            log.error("Error dumping log for {}", containerName, e);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.info("Interrupted dumping log from container {}", containerName, ie);
        }
    }

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    public static void dumpContainerLogFileToTarget(final DockerClient dockerClient,
                                                    final String containerId,
                                                    final String path) {
        final InspectContainerResponse inspectContainerResponse = dockerClient.inspectContainerCmd(containerId).exec();
        // docker api returns names prefixed with "/", it's part of it's legacy design,
        // this removes it to be consistent with what docker ps shows.
        final String containerName = inspectContainerResponse.getName().replace("/", "");

        try (final TarArchiveInputStream tarInputStream = new TarArchiveInputStream(
                dockerClient
                .copyArchiveFromContainerCmd(containerId, path)
                .exec(), CharsetNames.UTF_8)) {
            final Path p = Paths.get(path);
            final File output = new File(
                getTargetDirectory(containerName),
                p.getFileName().toString().replace("/", "-"));
            log.info("Dumping log file for : {} to target {}", containerName, output.getAbsolutePath());
            tarInputStream.getNextTarEntry();
            IOUtils.copy(tarInputStream, new FileOutputStream(output));
        } catch (IOException | NotFoundException e) {
            log.error("Error reading log file from container {}", containerName, e);

        }
    }

    public static ExecResult runCommand(final DockerClient docker,
                                        final String containerId,
                                        final String... cmd) throws Exception {
        return runCommand(docker, containerId, false, cmd);
    }

    public static ExecResult runCommand(final DockerClient docker,
                                        final String containerId,
                                        final boolean ignoreError,
                                        final String... cmd) throws Exception {
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        final InspectContainerResponse inspectContainerResponse = docker.inspectContainerCmd(containerId).exec();
        final String containerName = inspectContainerResponse.getName().replace("/", "");
        final String execId = docker.execCreateCmd(containerId)
            .withCmd(cmd)
            .withAttachStderr(true)
            .withAttachStdout(true)
            .exec()
            .getId();
        final String cmdString = Arrays.stream(cmd).collect(Collectors.joining(" "));
        final StringBuilder stdout = new StringBuilder();
        final StringBuilder stderr = new StringBuilder();
        docker.execStartCmd(execId).withDetach(false).exec(new ResultCallback<Frame>() {
                @Override
                public void close() {}

                @Override
                public void onStart(Closeable closeable) {
                    log.info("DOCKER.exec({}:{}): Executing...", containerName, cmdString);
                }

                @Override
                public void onNext(Frame object) {
                    log.info("DOCKER.exec({}:{}): {}", containerName, cmdString, object);
                    if (StreamType.STDOUT == object.getStreamType()) {
                        stdout.append(new String(object.getPayload(), UTF_8));
                    } else if (StreamType.STDERR == object.getStreamType()) {
                        stderr.append(new String(object.getPayload(), UTF_8));
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    future.completeExceptionally(throwable);
                }

                @Override
                public void onComplete() {
                    log.info("DOCKER.exec({}:{}): Done", containerName, cmdString);
                    future.complete(true);
                }
            });
        future.get();

        InspectExecResponse resp = docker.inspectExecCmd(execId).exec();
        while (resp.isRunning()) {
            Thread.sleep(200);
            resp = docker.inspectExecCmd(execId).exec();
        }
        int retCode = resp.getExitCode();
        if (retCode != 0) {
            if (!ignoreError) {
                log.error("DOCKER.exec({}:{}): failed with {} :\nStdout:\n{}\n\nStderr:\n{}",
                        containerName, cmdString, retCode, stdout.toString(), stderr.toString());
                throw new Exception(String.format("cmd(%s) failed on %s with exit code %d",
                    cmdString, containerId, retCode));
            } else {
                log.error("DOCKER.exec({}:{}): failed with {}", containerName, cmdString, retCode);
            }
        } else {
            log.info("DOCKER.exec({}:{}): completed with {}", containerName, cmdString, retCode);

        }
        return ExecResult.of(
            retCode,
            stdout.toString(),
            stderr.toString()
        );
    }
}
