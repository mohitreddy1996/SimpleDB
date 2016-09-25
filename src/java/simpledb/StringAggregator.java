package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbField;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> groups;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        groups = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key;
        int currentAggregator;

        if(gbField == Aggregator.NO_GROUPING){
            key = new IntField(Aggregator.NO_GROUPING);
        }else{
            key = tup.getField(gbField);
        }

        if(groups.containsKey(key)){
            currentAggregator = groups.get(key);
        }else{
            groups.put(key, 0);
        }

        currentAggregator = groups.get(key);

        currentAggregator++;
        groups.put(key, currentAggregator);

    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */

    public TupleDesc getTupleDesc(){
        Type[] typeArr;
        String[] fieldArr;

        TupleDesc td;

        if(gbField == Aggregator.NO_GROUPING){
            typeArr = new Type[1];
            fieldArr = new String[1];

            typeArr[0] = Type.INT_TYPE;
            fieldArr[0] = "";
        }else{
            typeArr = new Type[2];
            fieldArr = new String[2];

            typeArr[0] = gbfieldtype;
            typeArr[1] = Type.INT_TYPE;

            fieldArr[0] = "";
            fieldArr[1] = "";
        }

        td = new TupleDesc(typeArr, fieldArr);
        return td;
    }
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        TupleDesc td = this.getTupleDesc();

        if(gbField == Aggregator.NO_GROUPING){
            for(Field key : groups.keySet()){
                int value = groups.get(key);
                Tuple tuple = new Tuple(td);
                tuple.setField(0, new IntField(value));
                tuples.add(tuple);
            }
        }else {
            for (Field key : groups.keySet()){
                int value = groups.get(key);
                Tuple tuple = new Tuple(td);
                tuple.setField(0, key);
                tuple.setField(1, new IntField(value));
                tuples.add(tuple);
            }
        }

        return new TupleIterator(td, tuples);
    }

}
