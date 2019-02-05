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
package org.apache.pulsar.tests.common.framework;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.common.framework.SystemTestSuite.ServiceProvider;
import org.apache.pulsar.tests.common.framework.TestInvokers.InvokerType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.internal.runners.model.EachTestNotifier;
import org.junit.internal.runners.statements.RunAfters;
import org.junit.internal.runners.statements.RunBefores;
import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

/**
 * A test runner is used as a runner to run {@link SystemTest}. A {@link SystemTest} is usually run as part of a
 * {@link SystemTestSuite} where the test suite will instantiate a {@link Service} for the service test to test.
 * However a test suite might contain multiple service tests, it is a bit hard to run a specific test within a test
 * suite. This test runner is designed to be flexible to run a single service test.
 *
 * <p>To use it, annotate a class with <code>@RunWith(SystemTestRunner.class)</code> and
 * <code>@TestSuiteClass(MyTestSuiteClass.class)</code>. When you run the test annotated with this runner, it will
 * use the suite class specified at <tt>@TestSuiteClass</tt> to setup service and then run the actual test.
 */
@Slf4j
public class SystemTestRunner<T extends Service> extends BlockJUnit4ClassRunner {

    /**
     * Annotation to annotate a test case to be invoked by {@link SystemTestRunner}.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Inherited
    public @interface TestSuiteClass {
        Class<?> value();
    }

    private static <T> Class<T> getTestSuiteClass(Class<T> klass) throws InitializationError {
        TestSuiteClass annotation = klass.getAnnotation(TestSuiteClass.class);
        if (annotation == null) {
            throw new InitializationError(String.format("A service test '%s' should be annotated with a suite",
                klass.getName()));
        }
        return (Class<T>) annotation.value();
    }

    private static <T extends Service> SystemTestSuite<T> newSystemTestSuite(Class<?> klass)
            throws InitializationError {
        Class<?> suiteClass = getTestSuiteClass(klass);
        SystemTestSuite testSuite = new SystemTestSuite(suiteClass, new Class<?>[] { klass });
        return testSuite;
    }

    private static <T extends Service> Class<T> getServiceClass(Class<?> klass)
            throws InitializationError {
        Class<?> suiteClass = getTestSuiteClass(klass);
        return SystemTestSuite.getServiceClass(suiteClass);
    }

    private final Class<T> serviceClass;
    private final SystemTestSuite testSuite;
    private final boolean ownTestSuite;

    public SystemTestRunner(Class<?> klass) throws InitializationError {
        this(klass, getServiceClass(klass),  newSystemTestSuite(klass), true);
    }

    /**
     * Creates a BlockJUnit4ClassRunner to run {@code klass}.
     *
     * @param klass class to test
     * @param serviceClass class of the service to initialize
     * @param testSuite the test suite to run system tests
     * @throws InitializationError if the test class is malformed.
     */
    public SystemTestRunner(Class<?> klass,
                            Class<T> serviceClass,
                            SystemTestSuite<T> testSuite) throws InitializationError {
        this(klass, serviceClass, testSuite, false);
    }

    public SystemTestRunner(Class<?> klass,
                            Class<T> serviceClass,
                            SystemTestSuite<T> testSuite,
                            boolean ownTestSuite) throws InitializationError {
        super(klass);
        this.serviceClass = serviceClass;
        this.testSuite = testSuite;
        this.ownTestSuite = ownTestSuite;

        if (!getTestClass().isANonStaticInnerClass() && hasOneConstructor()) {
            Class<?>[] paremterTypes = getTestClass().getOnlyConstructor().getParameterTypes();
            if (paremterTypes.length != 1) {
                throw new RuntimeException("Test class " + getTestClass()
                    + "should have exactly one public one-argument constructor");
            } else if (!serviceClass.isAssignableFrom(paremterTypes[0])){
                throw new RuntimeException("Test class " + getTestClass()
                    + " should have argument " + serviceClass + " in constructor, but found argument : "
                    + paremterTypes[0]);
            }
        }
    }

    @Override
    protected Statement classBlock(RunNotifier notifier) {
        Statement statement = super.classBlock(notifier);
        if (ownTestSuite) {
            List<FrameworkMethod> testSuiteBefores = testSuite.getTestClass().getAnnotatedMethods(BeforeClass.class);
            statement = new RunBefores(statement, testSuiteBefores, null);
            List<FrameworkMethod> testSuiteAfters = testSuite.getTestClass().getAnnotatedMethods(AfterClass.class);
            return new RunAfters(statement, testSuiteAfters, null);
        } else {
            return statement;
        }
    }

    @Override
    protected void validateConstructor(List<Throwable> errors) {
        validateOnlyOneConstructor(errors);
    }

    private boolean hasOneConstructor() {
        return getTestClass().getJavaClass().getConstructors().length == 1;
    }

    @Override
    protected Object createTest() throws Exception {
        return getTestClass().getOnlyConstructor().newInstance(getServiceFromSuite());
    }

    T getServiceFromSuite() throws Exception {
        List<FrameworkMethod> services = testSuite.getTestClass().getAnnotatedMethods(ServiceProvider.class);
        if (services.isEmpty()) {
            log.error("No '@Service' annotation found in the system test suite : {}",
                testSuite.getTestClass().getName());
            throw new RuntimeException("No '@Service' annotation found in the system test suite : "
                + testSuite.getTestClass());
        } else if (services.size() > 1) {
            log.error("More than one '@Service' annotation found in the system test suite : {}",
                testSuite.getTestClass().getName());
            throw new RuntimeException("More than one '@Service' annotation found in the system test suite : "
                + testSuite.getTestClass());
        }
        Object service = services.get(0).getMethod().invoke(null);
        return serviceClass.cast(service);
    }

    @Override
    protected void runChild(FrameworkMethod method, RunNotifier notifier) {
        Description description = describeChild(method);
        if (isIgnored(method)) {
            // the test is annotated with <tt>@Ignored</tt>
            notifier.fireTestIgnored(description);
        } else {
            InvokerType invokerType = FrameworkUtils.getSystemTestInvoker();
            invokeTest(invokerType, method, notifier);
        }
    }

    private void invokeTest(InvokerType invokerType, FrameworkMethod method, RunNotifier notifier) {
        if (null == invokerType || InvokerType.LOCAL == invokerType) {
            runLeaf(methodBlock(method), describeChild(method), notifier);
        } else {
            EachTestNotifier eachTestNotifier = new EachTestNotifier(notifier, describeChild(method));
            try {
                eachTestNotifier.fireTestStarted();
                invokeTestAsync(invokerType, method.getMethod()).get();
            } catch (Throwable e) {
                eachTestNotifier.addFailure(e);
            } finally {
                eachTestNotifier.fireTestFinished();
            }
        }
    }

    private CompletableFuture<Void> invokeTestAsync(InvokerType type, Method method) {
        TestInvokers invokers = new TestInvokers();
        return invokers.getTestInvoker(type).invokeAsync(method);
    }
}
