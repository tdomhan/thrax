package edu.jhu.thrax.hadoop.features;

import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import edu.jhu.thrax.hadoop.datatypes.RuleWritable;
import edu.jhu.thrax.util.Vocabulary;

public class WordCountDifferenceFeature implements SimpleFeature {

  public static final String NAME = "word_count_difference";

  private static final IntWritable ZERO = new IntWritable(0);

  public Writable score(RuleWritable r) {
    int word_difference = 0;
    for (int tok : r.source)
      if (!Vocabulary.nt(tok)) word_difference--;
    for (int tok : r.target)
      if (!Vocabulary.nt(tok)) word_difference++;
    return new IntWritable(word_difference);
  }

  public String getName() {
    return NAME;
  }

  public void unaryGlueRuleScore(int nt, Map<Integer, Writable> map) {
    map.put(Vocabulary.id(NAME), ZERO);
  }

  public void binaryGlueRuleScore(int nt, Map<Integer, Writable> map) {
    map.put(Vocabulary.id(NAME), ZERO);
  }
}
