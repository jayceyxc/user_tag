package com.bcdata.analysis.raduis_user;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * CompositeKeyComparator
 *
 * The purpose of this class is to enable comparison of two CompositeKey(s).
 *
 *  @author yuxuecheng
 */
public class CompositeKeyComparator extends WritableComparator {

    protected CompositeKeyComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare (WritableComparable wc1, WritableComparable wc2) {
        CompositeKey ck1 = (CompositeKey)wc1;
        CompositeKey ck2 = (CompositeKey)wc2;

        int comparison = ck1.getDas ().compareTo (ck2.getDas ());
        if (comparison == 0) {
            // adsl are equal here
            return ck1.getAdsl ().compareTo (ck2.getAdsl ());
        } else {
            return comparison;
        }
    }
}
