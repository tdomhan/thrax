package edu.jhu.thrax.hadoop.features.pivot;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.FloatWritable;

import edu.jhu.thrax.hadoop.datatypes.FeatureMap;
import edu.jhu.thrax.hadoop.features.mapred.SourcePhraseGivenLHSFeature;
import edu.jhu.thrax.hadoop.features.mapred.TargetPhraseGivenLHSFeature;

public class PivotedSourcePhraseGivenLHSFeature extends NonAggregatingPivotedFeature {

  public static final String NAME = SourcePhraseGivenLHSFeature.NAME;

  public String getName() {
    return NAME;
  }

  public Set<String> getPrerequisites() {
    Set<String> prereqs = new HashSet<String>();
    prereqs.add(TargetPhraseGivenLHSFeature.NAME);
    return prereqs;
  }

  public FloatWritable pivot(FeatureMap src, FeatureMap tgt) {
    return new FloatWritable(((FloatWritable) src.get(TargetPhraseGivenLHSFeature.NAME)).get());
  }

  @Override
  public Set<String> getLowerBoundLabels() {
    Set<String> lower_bound_labels = new HashSet<String>();
    lower_bound_labels.add(TargetPhraseGivenLHSFeature.NAME);
    return lower_bound_labels;
  }

  @Override
  public Set<String> getUpperBoundLabels() {
    return null;
  }
}
