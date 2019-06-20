package org.kie.remote;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.kie.api.runtime.ObjectFilter;

public interface RemoteCepKieSession {

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
     */
    void insert(Object object);


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
     * @return all facts from the current session as a Collection.
     */
    CompletableFuture<Collection<? extends Object>> getObjects();

    /**
     * @param filter the filter to be applied to the returned collection of facts.
     * @return all facts from the current session that are accepted by the given <code>ObjectFilter</code>.
     */
    CompletableFuture<Collection<? extends Object>> getObjects(ObjectFilter filter);

    /**
     * @return the total number of facts currently in this entry point
     */
    CompletableFuture<Long> getFactCount();
}
