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
package org.apache.bookkeeper.api.segment;

/**
 * Schema to deserialize a {@link Record} to an object.
 */
public interface Schema<T> {

    /**
     * Deserialize a {@link Record} into an object.
     *
     * @param record a record to deserialize
     * @return a deserialized object.
     */
    T deserialize(Record record);

    /**
     * Get the field from a deserialized <tt>record</tt>.
     *
     * @param record the deserialized record
     * @param fieldIndex field index
     * @return the field value
     */
    Object getFieldValue(T record, int fieldIndex);

}
