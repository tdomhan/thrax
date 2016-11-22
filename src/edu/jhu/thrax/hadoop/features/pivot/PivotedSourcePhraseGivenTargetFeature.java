package edu.jhu.thrax.hadoop.features.pivot;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.FloatWritable;

import edu.jhu.thrax.hadoop.datatypes.FeatureMap;
import edu.jhu.thrax.hadoop.features.mapred.SourcePhraseGivenTargetFeature;
import edu.jhu.thrax.hadoop.features.mapred.TargetPhraseGivenSourceFeature;

public class PivotedSourcePhraseGivenTargetFeature extends PivotedNegLogProbFeature {

  public static final String NAME = SourcePhraseGivenTargetFeature.NAME;

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
    float src_f = ((FloatWritable) src.get(TargetPhraseGivenSourceFeature.NAME)).get();
    float f_tgt = ((FloatWritable) tgt.get(SourcePhraseGivenTargetFeature.NAME)).get();

    return new FloatWritable(src_f + f_tgt);
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
