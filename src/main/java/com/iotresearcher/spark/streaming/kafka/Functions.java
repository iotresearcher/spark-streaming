package com.iotresearcher.spark.streaming.kafka;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class Functions {

    public static Function2<Integer, Integer, Integer> INTEGER_SUM_REDUCER =
            new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer a, Integer b) throws Exception {
                    return a+b;
                }
            };

    public static class IntegerComparator implements Comparator<Integer>, Serializable {
        @Override
        public int compare(Integer a, Integer b) {
            if (a > b) return 1;
            if (a.equals(b)) return 0;
            return -1;
        }
    }

    public static Comparator<Integer> INTEGER_NATURAL_ORDER_COMPARATOR = new IntegerComparator();

}
