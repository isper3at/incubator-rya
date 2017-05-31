package org.apache.rya.indexing.pcj.storage.mongo;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.DELIM_BYTES;

import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaContext;
import org.apache.rya.api.resolver.RyaTypeResolverException;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.bson.conversions.Bson;
import org.openrdf.query.BindingSet;

/**
 * Converts a Pcj for storage in mongoDB or retrieval from mongoDB.
 */
public class MongoPcjAdapter implements BindingSetConverter<Bson> {
    @Override
    public Bson convert(final BindingSet bindingSet, final VariableOrder varOrder) throws BindingSetConversionException {
        checkNotNull(bindingSet);
        checkNotNull(varOrder);

        try {
            for(final String varName: varOrder) {
                // Only write information for a variable name if the binding set contains it.
                if(bindingSet.hasBinding(varName)) {
                    final RyaType rt = RdfToRyaConversions.convertValue(bindingSet.getBinding(varName).getValue());
                    final byte[][] serializedVal = RyaContext.getInstance().serializeType(rt);
                    byteSegments.add(serializedVal[0]);
                    byteSegments.add(serializedVal[1]);
                }

                // But always write the value delimiter. If a value is missing, you'll see two delimiters next to each-other.
                byteSegments.add(DELIM_BYTES);
            }

            return concat(byteSegments);
        } catch (final RyaTypeResolverException e) {
            throw new BindingSetConversionException("Could not convert the BindingSet into a byte[].", e);
        }
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public BindingSet convert(final Bson bindingSet, final VariableOrder varOrder)
            throws org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException {
        // TODO Auto-generated method stub
        return null;
    }
}
