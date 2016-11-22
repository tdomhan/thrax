package edu.jhu.thrax.hadoop.features.pivot;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Writable;

import edu.jhu.thrax.hadoop.datatypes.AlignmentWritable;
import edu.jhu.thrax.hadoop.datatypes.Annotation;
import edu.jhu.thrax.hadoop.datatypes.FeatureMap;
import edu.jhu.thrax.hadoop.features.annotation.AlignmentFeature;
import edu.jhu.thrax.hadoop.features.annotation.AnnotationPassthroughFeature;

public class PivotedAnnotationFeature implements PivotedFeature {

  public static final String NAME = "annotation"; 
  
  private Annotation aggregated = null;

  public String getName() {
    return NAME;
  }

  public Set<String> getPrerequisites() {
    Set<String> prereqs = new HashSet<String>();
    prereqs.add(AlignmentFeature.NAME);
    return prereqs;
  }

  public Annotation pivot(FeatureMap src, FeatureMap tgt) {
    AlignmentWritable src_f2e = ((AlignmentWritable) src.get(AlignmentFeature.NAME));
    AlignmentWritable tgt_f2e = ((AlignmentWritable) tgt.get(AlignmentFeature.NAME));

    return new Annotation(src_f2e.join(tgt_f2e));
  }

  public void unaryGlueRuleScore(int nt, Map<Integer, Writable> map) {}

  public void binaryGlueRuleScore(int nt, Map<Integer, Writable> map) {}

  public void initializeAggregation() {
    aggregated = null;
  }

  public void aggregate(FeatureMap a) {
    Annotation annotation = (Annotation) a.get(AnnotationPassthroughFeature.NAME);
    if (aggregated == null) {
      aggregated = new Annotation(annotation);
    } else {
      aggregated.setAlignment(aggregated.f2e().intersect(annotation.f2e()));
      aggregated.merge(annotation);
    }
  }

  public Annotation finalizeAggregation() {
    return aggregated;
  }

  @Override
  public Set<String> getLowerBoundLabels() {
    return null;
  }

  @Override
  public Set<String> getUpperBoundLabels() {
    return null;
  }
}
