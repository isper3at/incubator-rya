package mvm.rya.indexing.accumulo;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.openrdf.query.BindingSet;

/**
 * Decorates a {@link BindingSet} with a collection of visibilities.
 */
public class VisibilityBindingSet extends BindingSetDecorator {
    private static final long serialVersionUID = 1L;
    private final Set<String> visibilitySet;

    /**
     * @param set - Decorates the {@link BindingSet} with no visibilities.
     */
    public VisibilityBindingSet(BindingSet set) {
        this(set, new HashSet<String>());
    }

    /**
     * Creates a new {@link VisibilityBindingSet} 
     * @param set - The {@link BindingSet} to decorate
     * @param visibility - The set of visibilities on the {@link BindingSet} (not null)
     */
    public VisibilityBindingSet(BindingSet set, Set<String> visibility) {
        super(set);
        this.visibilitySet = checkNotNull(visibility);
    }

    /**
     * @return - The Visibilities on the {@link BindingSet}
     */
    public Collection<String> getVisibility() {
        return visibilitySet;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if(o instanceof VisibilityBindingSet) {
            VisibilityBindingSet other = (VisibilityBindingSet) o;
            return super.equals(other) && visibilitySet.equals(other.getVisibility());
        }
        return false;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(super.toString());
        sb.append("\n  Visibility: ");
        for(String str : getVisibility()) {
            sb.append(str + ", ");
        }
        sb.append("\n");
        return sb.toString();
    }
}