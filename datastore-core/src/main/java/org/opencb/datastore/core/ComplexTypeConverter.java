package org.opencb.datastore.core;

/**
 * Converts between a Java class and a type dependant of the storage system used.
 * 
 * @author Cristina Yenyxe Gonzalez Garcia &lt;cyenyxe@ebi.ac.uk&gt;
 */
public interface ComplexTypeConverter<DataModelType, StorageType> {
    
    public DataModelType convertToDataModelType(StorageType object);
    
    public StorageType convertToStorageType(DataModelType object);
    
}
