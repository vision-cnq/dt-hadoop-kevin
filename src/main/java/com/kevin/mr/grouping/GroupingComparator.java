package com.kevin.mr.grouping;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author kevin
 * @version 1.0
 * @description     在分组比较的时候，只比较原来的key，而不是比较组合的key
 * @createDate 2018/12/19
 */
public class GroupingComparator implements RawComparator<IntPair> {

    // 一个字节一个字节的比，直到找到一个不相同的字节时比较这个字节的大小作为两个字节流的大小比较结果。
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return WritableComparator.compareBytes(b1,s1,Integer.SIZE/8,b2,s2,Integer.SIZE/8);
    }

    @Override
    public int compare(IntPair o1, IntPair o2) {
        int first1 = o1.getFirst();
        int first2 = o2.getFirst();
        return first1 - first2;
    }
}

/**
 *
 //第二种方法，继承WritableComparator
 public static class GroupingComparator extends WritableComparator{
     protected GroupingComparator(){

     super(NewKey.class, true);
        }

     @Override
     //Compare two WritableComparables.
     public int compare(WritableComparable w1, WritableComparable w2){

        NewKey nk1 = (NewKey) w1;
        NewKey nk2 = (NewKey) w2;
        int l = nk1.getFirst();
        int r = nk2.getFirst();
        return l == r ? 0 : (l < r ? -1 : 1);
     }
 }
 */
