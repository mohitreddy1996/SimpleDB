package simpledb;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * Own fields added for the TupleDesc
    **/
    private ArrayList<Type> typeList;
    private ArrayList<String> fieldNameList;
    private ArrayList<TDItem> tdItems;


    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        this.tdItems = new ArrayList<>();
        this.typeList = new ArrayList<>();
        this.fieldNameList = new ArrayList<>();
        for(int i = 0; i<typeAr.length; i++) {
            TDItem tdItem = new TDItem(typeAr[i], fieldAr[i]);
            this.tdItems.add(tdItem);
            this.typeList.add(typeAr[i]);
            this.fieldNameList.add(fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this.tdItems = new ArrayList<>();
        for (Type aTypeAr : typeAr) {
            tdItems.add(new TDItem(aTypeAr, ""));
        }
    }

    /**
     *
     * Empty Constructor;
     * @return
     */
    public TupleDesc(){
        this.typeList = new ArrayList<>();
        this.fieldNameList = new ArrayList<>();
        this.tdItems = new ArrayList<>();
    }

    /**
     * HELPER FUNCTIONS FOR TUPLEDESC.
     *
     */
    public ArrayList<Type> getTypeList(){
        return this.typeList;
    }

    public ArrayList<String> getFieldNameList(){
        return this.fieldNameList;
    }

    public ArrayList<TDItem> getTDItems(){
        return this.tdItems;
    }

    public void putTypeItem(Type type){
        this.typeList.add(type);
    }

    public void putFieldName(String name){
        this.fieldNameList.add(name);
    }

    public void putTDItem(TDItem tdItem){
        this.tdItems.add(tdItem);
    }

    public String getReqString(TDItem idItem, int index){
        String ans = "";
        if(index != 0){
            ans+=",";
        }
        ans+= idItem.fieldType + "(" + idItem.fieldName + ")";
        return ans;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        int typeArraySize = tdItems.size();
        if(i>typeArraySize){
            throw new NoSuchElementException("Index out of Range");
        }
        return tdItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        int typeArraySize = tdItems.size();
        if(i>typeArraySize){
            throw new NoSuchElementException("Index out of range");
        }
        return tdItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        int index = 0;
        if(tdItems == null){
            throw new NoSuchElementException("");
        }
        for (TDItem tdItem: tdItems
             ) {
            if(tdItem!=null && Objects.equals(tdItem.fieldName, name)){
                return index;
            }
            index++;
        }
        throw new NoSuchElementException("Element doesnt exist");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int sizeBytes = 0;
        for (TDItem tdItem: tdItems
             ) {
            if(tdItem.fieldType == Type.INT_TYPE){
                sizeBytes += Type.INT_TYPE.getLen();
            }else{
                sizeBytes += Type.STRING_TYPE.getLen();
            }
        }
        return sizeBytes;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        TupleDesc mergeTupleDesc = new TupleDesc();
        for (TDItem tdItem: td1.getTDItems()
             ) {
            mergeTupleDesc.putTDItem(tdItem);
        }
        for (TDItem tdItem: td2.getTDItems()
             ) {
            mergeTupleDesc.putTDItem(tdItem);
        }
        return mergeTupleDesc;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        TupleDesc tuple;
        try {
            tuple = (TupleDesc) o;
        } catch (Exception e){
            return false;
        }
        if(tuple == null){
            return false;
        }
        ArrayList<TDItem> objectTypeList = tuple.getTDItems();
        if(objectTypeList.size() != this.tdItems.size()){
            return false;
        }
        for(int i=0; i<objectTypeList.size(); i++){
            if(objectTypeList.get(i).fieldType != this.tdItems.get(i).fieldType){
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String resString= "";
        int index = 0;
        for (TDItem tdItem: this.tdItems
             ) {
            resString += getReqString(tdItem, index);
            index++;
        }
        return resString;
    }
}
