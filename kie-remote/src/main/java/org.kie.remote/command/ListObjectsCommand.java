package org.kie.remote.command;

import java.util.Collection;

import org.kie.remote.RemoteFactHandle;

public class ListObjectsCommand extends WorkingMemoryActionCommand implements Visitable {

    private Collection<? extends Object> objects;

    /* Empty constructor for serialization */
    public ListObjectsCommand() { }

    public ListObjectsCommand(RemoteFactHandle factHandle, String entryPoint) {
        super(factHandle, entryPoint);
    }

    @Override
    public void accept(Visitor visitor) { visitor.visit(this); }

    @Override
    public String toString() {
        return "Update of " + getFactHandle() + " from entry-point " + getEntryPoint();
    }



}
