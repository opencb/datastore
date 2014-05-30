package org.opencb.datastore.core;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public interface ComplexTypeConverter<Type1, Type2> {
    
    public Type2 convert(Type1 object);
    
}
