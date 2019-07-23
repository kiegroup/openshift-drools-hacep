/*
 * Copyright 2019 Red Hat
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

package org.kie.remote;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

public interface RemoteEntryPoint<T> {

    /**
     * @return the String Id of this entry point
     */
    String getEntryPointId();

    /**
     * Inserts a new fact into this entry point
     *
     * @param object
     *        the fact to be inserted
     *
     * @return the fact handle created for the given fact
     */
    RemoteFactHandle insert(Object object);

    /**
     * Retracts the fact for which the given FactHandle was assigned
     * regardless if it has been explicitly or logically inserted.
     *
     * @param handle the handle whose fact is to be retracted.
     */
    void delete(RemoteFactHandle handle);

    /**
     * Updates the fact for which the given FactHandle was assigned with the new
     * fact set as the second parameter in this method.
     *
     * @param handle the FactHandle for the fact to be updated.
     * @param object the new value for the fact being updated.
     */
    void update(RemoteFactHandle handle, Object object);

    /**
     * <p>This class is <i>not</i> a general-purpose <tt>Collection</tt>
     * implementation!  While this class implements the <tt>Collection</tt> interface, it
     * intentionally violates <tt>Collection</tt> general contract, which mandates the
     * use of the <tt>equals</tt> method when comparing objects.</p>
     *
     * <p>Instead the approach used when comparing objects with the <tt>contains(Object)</tt>
     * method is dependent on the WorkingMemory configuration, where it can be configured for <tt>Identity</tt>
     * or for <tt>Equality</tt>.</p>
     *
     * @@return CompletableFuture of Collection<? extends Object>to read all facts from the current session as a Collection.
     */
    CompletableFuture<Collection<? extends Object>> getObjects();

    /**
     * @param filter the filter to be applied to the returned collection of facts.
     * @@return CompletableFuture of Collection<? extends Object> to read all facts from the current session that are accepted by the given <code>ObjectFilter</code>.
     */
    CompletableFuture<Collection<? extends Object>> getObjects(Predicate<Object> filter);

    /**
     * @@return CompletableFuture of Long to read the total number of facts currently in this entry point
     */
    CompletableFuture<Long> getFactCount();
}
