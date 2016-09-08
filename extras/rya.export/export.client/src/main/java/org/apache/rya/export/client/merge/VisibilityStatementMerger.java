package org.apache.rya.export.client.merge;

import org.apache.rya.export.api.MergerException;
import org.apache.rya.export.api.StatementMerger;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;

import mvm.rya.api.domain.RyaStatement;

public class VisibilityStatementMerger implements StatementMerger {
    @Override
    public Optional<RyaStatement> merge(final Optional<RyaStatement> parent, final Optional<RyaStatement> child)
            throws MergerException {
        if(parent.isPresent()) {
            final RyaStatement parentStatement = parent.get();
            if(child.isPresent()) {
                final RyaStatement childStatement = child.get();
                final String pVis = new String(parentStatement.getColumnVisibility());
                final String cVis = new String(childStatement.getColumnVisibility());
                String visibility = "";
                final Joiner join = Joiner.on(")&(");
                if(pVis.isEmpty() || cVis.isEmpty()) {
                    visibility = (pVis + cVis).trim();
                } else {
                    visibility = "(" + join.join(pVis, cVis) + ")";
                }
                parentStatement.setColumnVisibility(visibility.getBytes());
                return Optional.of(parentStatement);
            }
            return parent;
        } else if(child.isPresent()) {
            return child;
        }
        return Optional.absent();
    }
}
