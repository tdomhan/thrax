package edu.jhu.thrax.hadoop.features;

import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import edu.jhu.thrax.hadoop.datatypes.RuleWritable;
import edu.jhu.thrax.util.Vocabulary;

public class MonotonicFeature implements SimpleFeature {

  public static final String NAME = "monotonic";

  private static final IntWritable ZERO = new IntWritable(0);
  private static final IntWritable ONE = new IntWritable(1);

  public Writable score(RuleWritable r) {
    return (r.monotone ? ONE : ZERO);
  }

  public String getName() {
    return NAME;
  }

  public void unaryGlueRuleScore(int nt, Map<Integer, Writable> map) {
    map.put(Vocabulary.id(NAME), ONE);
  }

  public void binaryGlueRuleScore(int nt, Map<Integer, Writable> map) {
    map.put(Vocabulary.id(NAME), ONE);
  }
}
