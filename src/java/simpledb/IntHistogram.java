package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {


    private int buckets;
    private int minVal, maxVal;
    private int numTups = 0;
    private int[] numElems;
    private int width;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.maxVal = max;
        this.minVal = min;

        numElems = new int[buckets];
        for(int i = 0; i<buckets; i++){
            numElems[i] = 0;
        }
        width = (int) Math.ceil(1.0*(max - min + 1)/buckets);

    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if(v < minVal){
            numElems[0]++;
        }else if(v > maxVal){
            numElems[buckets - 1]++;
        }else{
            int buckt = (v - minVal)/width;
            numElems[buckt]++;
        }
        numTups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        int bucketInd = (v-minVal)/width;

        int left = bucketInd*width + minVal;
        int right = bucketInd*width + minVal + width - 1;
    	// some code goes here
        if (op == Predicate.Op.EQUALS){
            if(v<minVal || v>maxVal){
                return 0.0;
            }else{
                int height = numElems[bucketInd];
                return (1.0*height/width)/numTups;
            }
        }else if(op == Predicate.Op.NOT_EQUALS){
            if(v<minVal || v>maxVal){
                return 1.0;
            }else{
                int height = numElems[bucketInd];
                return 1.0 - (1.0*height/width)/numTups;
            }
        }else if(op == Predicate.Op.LESS_THAN){
            if(v <= minVal){
                return 0.0;
            }else if(v > maxVal){
                return 1.0;
            }else{
                double ans = 0.0;
                int height = numElems[bucketInd];
                ans = (1.0*height/numTups) * ((v - left)/width);
                for(int hist = bucketInd - 1; hist >= 0; hist--){
                    ans += ((1.0*numElems[hist]/numTups));
                }

                return ans;
            }
        }else if(op == Predicate.Op.GREATER_THAN){
            if( v < minVal){
                return 1.0;
            }else if( v> maxVal - 1){
                return 0.0;
            }else{
                double ans = 0.0;
                int height = numElems[bucketInd];
                ans = (height/numTups) *((right - v)/width);
                for(int hist = bucketInd + 1; hist<buckets; hist++){
                    ans += (1.0*numElems[hist]/numTups);
                }

                return ans;
            }
        }else if(op == Predicate.Op.LESS_THAN_OR_EQ){
            if(v <= minVal){
                return 0.0;
            }else if(v > maxVal){
                return 1.0;
            }else{
                double ans = 0.0;
                int height = numElems[bucketInd];
                ans = (1.0*height/numTups) * (1.0*(v - left)/width);
                for(int hist = bucketInd - 1; hist >= 0; hist--){
                    ans += ((1.0*numElems[hist]/numTups));
                }
                ans += (1.0*height/width)/numTups;
                return ans;
            }
        }else if(op == Predicate.Op.GREATER_THAN_OR_EQ){
            if( v < minVal){
                return 1.0;
            }else if( v> maxVal - 1){
                return 0.0;
            }else{
                double ans = 0.0;
                int height = numElems[bucketInd];
                ans = (1.0*height/numTups) *(1.0*(right - v)/width);
                for(int hist = bucketInd + 1; hist<buckets; hist++){
                    ans += (1.0*numElems[hist]/numTups);
                }
                ans += (1.0*height/width)/numTups;
                return ans;
            }
        }
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }
}
