package mvm.rya.indexing.external;

import java.util.Iterator;
import java.util.Set;

import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;

/**
 * Abstracts out the decoration of a {@link BindingSet}.
 */
public abstract class BindingSetDecorator implements BindingSet {
    private static final long serialVersionUID = 1L;
    private final BindingSet set;

    public BindingSetDecorator(BindingSet set) {
        this.set = set;
    }

    @Override
    public Iterator<Binding> iterator() {
        return set.iterator();
    }

    @Override
    public Set<String> getBindingNames() {
        return set.getBindingNames();
    }

    @Override
    public Binding getBinding(String bindingName) {
        return set.getBinding(bindingName);
    }

    @Override
    public boolean hasBinding(String bindingName) {
        return set.hasBinding(bindingName);
    }

    @Override
    public Value getValue(String bindingName) {
        return set.getValue(bindingName);
    }

    @Override
    public int size() {
        return set.size();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("  names: ");
        for (String name : getBindingNames()) {
            sb.append("\n    [name]: " + name + "  ---  [value]: " + getBinding(name).getValue().toString());
        }
        return sb.toString();
    }
}
