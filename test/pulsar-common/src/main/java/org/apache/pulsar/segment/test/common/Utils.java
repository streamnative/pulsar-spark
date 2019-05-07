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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;

/**
 * Utils for testing.
 */
public final class Utils {

    public static String jarFinderGetJar(Class klass) {
        checkNotNull(klass, "klass");
        ClassLoader loader = klass.getClassLoader();
        if (loader != null) {
            String classFile = klass.getName().replaceAll("\\.", "/") + ".class";
            try {
                for (Enumeration itr = loader.getResources(classFile); itr.hasMoreElements();) {
                    URL url = (URL) itr.nextElement();
                    String path = url.getPath();
                    if (path.startsWith("file:")) {
                        path = path.substring("file:".length());
                    }
                    path = URLDecoder.decode(path, "UTF-8");
                    if ("jar".equals(url.getProtocol())) {
                        path = URLDecoder.decode(path, "UTF-8");
                        return path.replaceAll("!.*$", "");
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    private Utils() {}

}
