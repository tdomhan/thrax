package edu.jhu.thrax.hadoop.features.pivot;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.FloatWritable;

import edu.jhu.thrax.hadoop.datatypes.FeatureMap;
import edu.jhu.thrax.hadoop.features.mapred.SourcePhraseGivenTargetFeature;
import edu.jhu.thrax.hadoop.features.mapred.TargetPhraseGivenSourceFeature;

public class PivotedTargetPhraseGivenSourceFeature extends PivotedNegLogProbFeature {

  public static final String NAME = TargetPhraseGivenSourceFeature.NAME;

  public String getName() {
    return NAME;
  }

  public Set<String> getPrerequisites() {
    Set<String> prereqs = new HashSet<String>();
    prereqs.add(TargetPhraseGivenSourceFeature.NAME);
    prereqs.add(SourcePhraseGivenTargetFeature.NAME);
    return prereqs;
  }

  public FloatWritable pivot(FeatureMap src, FeatureMap tgt) {
    float tgt_f = ((FloatWritable) tgt.get(TargetPhraseGivenSourceFeature.NAME)).get();
    float f_src = ((FloatWritable) src.get(SourcePhraseGivenTargetFeature.NAME)).get();

    return new FloatWritable(tgt_f + f_src);
  }

  @Override
  public Set<String> getLowerBoundLabels() {
    Set<String> lower_bound_labels = new HashSet<String>();
    lower_bound_labels.add(TargetPhraseGivenSourceFeature.NAME);
    lower_bound_labels.add(SourcePhraseGivenTargetFeature.NAME);
    return lower_bound_labels;
  }

  @Override
  public Set<String> getUpperBoundLabels() {
    return null;
  }
}
