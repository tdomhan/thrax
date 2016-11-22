package edu.jhu.thrax.hadoop.features.pivot;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.FloatWritable;

import edu.jhu.thrax.hadoop.datatypes.FeatureMap;
import edu.jhu.thrax.hadoop.features.annotation.SourceGivenTargetLexicalProbabilityFeature;
import edu.jhu.thrax.hadoop.features.annotation.TargetGivenSourceLexicalProbabilityFeature;

public class PivotedLexicalSourceGivenTargetFeature extends PivotedNegLogProbFeature {

  public static final String NAME = SourceGivenTargetLexicalProbabilityFeature.NAME;

  public String getName() {
    return NAME;
  }

  public Set<String> getPrerequisites() {
    Set<String> prereqs = new HashSet<String>();
    prereqs.add(SourceGivenTargetLexicalProbabilityFeature.NAME);
    prereqs.add(TargetGivenSourceLexicalProbabilityFeature.NAME);
    return prereqs;
  }

  public FloatWritable pivot(FeatureMap src, FeatureMap tgt) {
    float egf = ((FloatWritable) tgt.get(TargetGivenSourceLexicalProbabilityFeature.NAME)).get();
    float fge = ((FloatWritable) src.get(SourceGivenTargetLexicalProbabilityFeature.NAME)).get();

    return new FloatWritable(egf + fge);
  }

  @Override
  public Set<String> getLowerBoundLabels() {
    Set<String> lower_bound_labels = new HashSet<String>();
    lower_bound_labels.add(TargetGivenSourceLexicalProbabilityFeature.NAME);
    lower_bound_labels.add(SourceGivenTargetLexicalProbabilityFeature.NAME);
    return lower_bound_labels;
  }

  @Override
  public Set<String> getUpperBoundLabels() {
    return null;
  }
}
