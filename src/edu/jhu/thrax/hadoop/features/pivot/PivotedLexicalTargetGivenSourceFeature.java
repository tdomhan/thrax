package edu.jhu.thrax.hadoop.features.pivot;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.FloatWritable;

import edu.jhu.thrax.hadoop.datatypes.FeatureMap;
import edu.jhu.thrax.hadoop.features.annotation.SourceGivenTargetLexicalProbabilityFeature;
import edu.jhu.thrax.hadoop.features.annotation.TargetGivenSourceLexicalProbabilityFeature;

public class PivotedLexicalTargetGivenSourceFeature extends PivotedNegLogProbFeature {

  public static final String NAME = TargetGivenSourceLexicalProbabilityFeature.NAME;

  public String getName() {
    return NAME;
  }

  public Set<String> getPrerequisites() {
    Set<String> prereqs = new HashSet<String>();
    prereqs.add(TargetGivenSourceLexicalProbabilityFeature.NAME);
    prereqs.add(SourceGivenTargetLexicalProbabilityFeature.NAME);
    return prereqs;
  }

  public FloatWritable pivot(FeatureMap src, FeatureMap tgt) {
    float egf = ((FloatWritable) src.get(TargetGivenSourceLexicalProbabilityFeature.NAME)).get();
    float fge = ((FloatWritable) tgt.get(SourceGivenTargetLexicalProbabilityFeature.NAME)).get();

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
