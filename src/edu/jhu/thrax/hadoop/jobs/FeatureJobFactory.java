package edu.jhu.thrax.hadoop.jobs;

import java.util.HashSet;
import java.util.Set;

import edu.jhu.thrax.hadoop.features.mapred.LexicalProbabilityFeature;
import edu.jhu.thrax.hadoop.features.mapred.MapReduceFeature;
import edu.jhu.thrax.hadoop.features.mapred.RarityPenaltyFeature;
import edu.jhu.thrax.hadoop.features.mapred.SourcePhraseGivenTargetFeature;
import edu.jhu.thrax.hadoop.features.mapred.TargetPhraseGivenSourceFeature;

public class FeatureJobFactory
{
    public static MapReduceFeature get(String name)
    {
        if (name.equals("e2fphrase"))
            return new SourcePhraseGivenTargetFeature();
        else if (name.equals("f2ephrase"))
            return new TargetPhraseGivenSourceFeature();
        else if (name.equals("rarity"))
            return new RarityPenaltyFeature();
        else if (name.equals("lexprob"))
            return new LexicalProbabilityFeature();
        return null;
    }
    
    public static Set<MapReduceFeature> getPrerequisiteFeatures(String name)
    {
    	Set<MapReduceFeature> needed_features = new HashSet<MapReduceFeature>();
    	if (name.equals("e2fphrase") || name.equals("f2ephrase")) {
            needed_features.add(new SourcePhraseGivenTargetFeature());
            needed_features.add(new TargetPhraseGivenSourceFeature());
    	}
        else if (name.equals("rarity"))
            needed_features.add(new RarityPenaltyFeature());
        else if (name.equals("lexprob"))
            needed_features.add(new LexicalProbabilityFeature());
        return needed_features;
    }
    
    public static Set<MapReduceFeature> getParaphrasingFeatures(String name)
    {
    	Set<MapReduceFeature> needed_features = new HashSet<MapReduceFeature>();
    	if (name.equals("e2fphrase") || name.equals("f2ephrase")) {
            needed_features.add(new SourcePhraseGivenTargetFeature());
            needed_features.add(new TargetPhraseGivenSourceFeature());
    	}
        else if (name.equals("rarity"))
            needed_features.add(new RarityPenaltyFeature());
        else if (name.equals("lexprob"))
            needed_features.add(new LexicalProbabilityFeature());
        return needed_features;
    }
}

