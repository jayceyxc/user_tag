package com.bcdata.analysis.dpcvisit_user;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * CompositeKey: represents a pair of
 * (String adsl, String das).
 *
 *
 * We do a primary grouping pass on the adsl field to get
 * all of the data of one type together, and then our "secondary sort"
 * during the shuffle phase uses the das value to sort
 * so that they arrive at the reducer partitioned
 * and in sorted order.
 *
 *
 */
public class CompositeKey implements WritableComparable<CompositeKey> {
    // natural key is adsl
    // composite key is a pair (das, adsl)
    private String das;
    private String adsl;

    public CompositeKey(String das, String adsl) {
        set(das, adsl);
    }

    public CompositeKey() {

    }

    public void set(String das, String adsl) {
        this.das = das;
        this.adsl = adsl;
    }

    public String getAdsl () {
        return this.adsl;
    }

    public String getDas () {
        return this.das;
    }

    @Override
    public void write (DataOutput out) throws IOException {
        out.writeUTF (das);
        out.writeUTF (adsl);
    }

    @Override
    public void readFields (DataInput in) throws IOException {
        this.das = in.readUTF ();
        this.adsl = in.readUTF ();
    }

    @Override
    public int compareTo (CompositeKey other) {
        if (this.das.compareTo (other.getDas ()) != 0) {
            return this.das.compareTo (other.getDas ());
        } else if (this.adsl.compareTo (other.getAdsl ()) != 0) {
            return this.adsl.compareTo (other.getAdsl ());
        } else {
            return 0;
        }
    }

    @Override
    public String toString () {
        return "das='" + das + '\'' +
                ", adsl='" + adsl + '\'';
    }
}
