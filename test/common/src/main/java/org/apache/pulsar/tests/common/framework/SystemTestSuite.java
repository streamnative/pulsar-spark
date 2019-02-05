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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.ParentRunner;
import org.junit.runners.model.InitializationError;

/**
 * Using <code>SystemTestSuite</code> as a runner allows you to manually build a suite
 * containing tests from many classes to test over a shared {@link Service}. To use it,
 * annotate a class with <code>@RunWith(SystemTestSuite.class)</code>,
 * <code>@SuiteClasses({TestClass1.class, ...})</code> and <code>@ServiceClass(TestService.class)</code>.
 * When you run this class, it will run all the tests in all the suite classes.
 *
 * <p>You can use the normal <tt>@BeforeClass</tt> and <tt>@AfterClass</tt> to setup and teardown the
 * <tt>Service</tt> that will be used by the service tests listed under <tt>@SuiteClasses</tt>. However you also
 * need to implement a static method returns an instance of <tt>Service</tt> specified by <tt>@ServiceClass</tt>.
 * This instance will be used as the <tt>Service</tt> being tested by the service tests.
 */
@Slf4j
public class SystemTestSuite<T extends Service> extends ParentRunner<Runner> {

    /**
     * Annotation class to annotate a test suite as a {@link SystemTestSuite}.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Inherited
    public @interface SuiteClasses {
        /**
         * Return the service tests to be run.
         *
         * @return the service tests to be run
         */
        Class<? extends SystemTest>[] value();
    }

    /**
     * Annotation class to annotate the service to initialize by {@link SystemTestSuite}.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Inherited
    public @interface ServiceClass {
        Class<? extends Service> value();
    }

    /**
     * Annotation class to annotate a class as a service provider.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface ServiceProvider {
    }

    private static <T> Class<T>[] getAnnotatedClasses(Class<?> klass) throws InitializationError {
        SuiteClasses annotation = klass.getAnnotation(SuiteClasses.class);
        if (annotation == null) {
            throw new InitializationError(String.format("class '%s' must have a SuiteClasses annotation",
                klass.getName()));
        }
        return (Class<T>[]) annotation.value();
    }

    static <T> Class<T> getServiceClass(Class<?> klass) throws InitializationError {
        ServiceClass annotation = klass.getAnnotation(ServiceClass.class);
        if (annotation == null) {
            throw new InitializationError(String.format("class '%s' must have a ServiceClass annotation",
                klass.getName()));
        }
        return (Class<T>) annotation.value();
    }

    private final List<Runner> runners;

    /**
     * Called reflectively on classes annotated with <code>@RunWith(SystemTestSuite.class)</code>.
     *
     * @param testClass the test suite class
     * @throws InitializationError when fail to initialize the system test suite
     */
    public SystemTestSuite(Class<?> testClass) throws InitializationError {
        this(testClass, getAnnotatedClasses(testClass));
    }

    public SystemTestSuite(Class<?> testClass, Class<?>[] serviceTestClasses) throws InitializationError {
        super(testClass);
        List<Runner> serviceTestRunners = new ArrayList<>(serviceTestClasses.length);
        Class<T> serviceClass = getServiceClass(testClass);
        for (Class<?> cls : serviceTestClasses) {
            Runner runner = new SystemTestRunner<>(cls, serviceClass, this);
            serviceTestRunners.add(runner);
        }
        this.runners = Collections.unmodifiableList(serviceTestRunners);
    }

    @Override
    protected List<Runner> getChildren() {
        return runners;
    }

    @Override
    protected Description describeChild(Runner child) {
        return child.getDescription();
    }

    @Override
    protected void runChild(Runner child, RunNotifier notifier) {
        child.run(notifier);
    }
}
