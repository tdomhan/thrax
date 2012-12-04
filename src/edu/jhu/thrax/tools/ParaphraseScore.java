package edu.jhu.thrax.tools;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.logging.Logger;

import edu.jhu.jerboa.util.FileManager;
import edu.jhu.thrax.ThraxConfig;
import edu.jhu.thrax.util.io.LineReader;

public class ParaphraseScore {

  private static final Logger logger = Logger.getLogger(ParaphraseScore.class.getName());

  private static final String DELIM = String.format(" %s ", ThraxConfig.DELIMITER_REGEX);
  
  private static int unknown_source;
  private static int total;
  private static int correct;
  private static int found;

  public static void main(String[] args) {

    String grammar_file = null;
    String reference_file = null;
    String weight_file = null;
    String output_file = null;

    boolean labeled = true;
    boolean sparse = true;

    for (int i = 0; i < args.length; i++) {
      if ("-g".equals(args[i]) && (i < args.length - 1)) {
        grammar_file = args[++i];
      } else if ("-r".equals(args[i]) && (i < args.length - 1)) {
        reference_file = args[++i];
      } else if ("-w".equals(args[i]) && (i < args.length - 1)) {
        weight_file = args[++i];
      } else if ("-o".equals(args[i]) && (i < args.length - 1)) {
        output_file = args[++i];
      } else if ("-l".equals(args[i])) {
        labeled = true;
      } else if ("-s".equals(args[i])) {
        sparse = true;
      }
    }

    if (grammar_file == null) {
      logger.severe("No grammar specified.");
      return;
    }
    if (reference_file == null) {
      logger.severe("No reference file specified.");
      return;
    }
    if (weight_file == null) {
      logger.severe("No weight file specified.");
      return;
    }
    if (output_file == null) {
      logger.severe("No output file specified.");
      return;
    }

    unknown_source = 0;

    HashMap<String, Integer> reference_pairs = new HashMap<String, Integer>();
    HashSet<String> sources = new HashSet<String>();
    HashMap<String, Double> weights = new HashMap<String, Double>();
    try {
      LineReader reference_reader = new LineReader(reference_file);
      while (reference_reader.hasNext()) {
        String line = reference_reader.next().trim();
        String[] fields = line.split(DELIM);
        if (reference_pairs.containsKey(line))
          reference_pairs.put(line, reference_pairs.get(line) + 1);
        else 
          reference_pairs.put(line, 1);
        sources.add(fields[0]);
      }
      reference_reader.close();

      LineReader weights_reader = new LineReader(weight_file);
      while (weights_reader.hasNext()) {
        String line = weights_reader.next().trim();
        if (line.isEmpty()) continue;
        String[] fields = line.split("\\s+");
        weights.put(fields[0], Double.parseDouble(fields[1]));
      }
      weights_reader.close();

      PriorityQueue<Entry> entries = new PriorityQueue<Entry>();
      
      LineReader reader = new LineReader(grammar_file);
      System.err.print("[");
      int count = 0;
      while (reader.hasNext()) {
        String rule_line = reader.next().trim();

        String[] fields = rule_line.split(DELIM);

        if (!fields[1].startsWith("[") || !fields[1].endsWith("]") || !fields[2].startsWith("[")
            || !fields[2].endsWith("]")) continue;

        String[] source_words = fields[1].split("\\s+");
        String[] target_words = fields[2].split("\\s+");

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < source_words.length; ++i) {
          if (source_words[i].endsWith(",1]"))
            source_words[i] = "[1]";
          else if (source_words[i].endsWith(",2]")) source_words[i] = "[2]";
          if (i > 0) builder.append(" ");
          builder.append(source_words[i]);
        }
        String source = builder.toString();
        builder = new StringBuilder();
        for (int i = 0; i < target_words.length; ++i) {
          if (target_words[i].endsWith(",1]"))
            target_words[i] = "[1]";
          else if (target_words[i].endsWith(",2]")) target_words[i] = "[2]";
          if (i > 0) builder.append(" ");
          builder.append(target_words[i]);
        }
        String target = builder.toString();

        if (!sources.contains(source)) {
          unknown_source++;
          continue;
        }
        
        double score = 0;
        String[] features = fields[3].split("\\s+");
        for (String f : features) {
          String[] parts = f.split("=");
          if (weights.containsKey(parts[0]))
            score += weights.get(parts[0]) * Double.parseDouble(parts[1]);
        }
        
        if (++count % 10000 == 0)
          System.err.print("-");
        
        entries.add(new Entry(source + " ||| " + target, score));
      }
      System.err.println("]");
      reader.close();

      total = entries.size();
      found = 0;
      correct = 0;
      for (int c: reference_pairs.values())
        correct += c;
      
      for (Entry e : entries) {
        if (reference_pairs.containsKey(e.pair))
          found += reference_pairs.get(e.pair);
      }
      
      System.err.println("Total: " + total);
      System.err.println("Found: " + found);
      System.err.println("Correct: " + correct);
      System.err.println("Not matching: " + unknown_source);
      
      BufferedWriter score_writer = FileManager.getWriter(output_file);
      while (!entries.isEmpty()) {
        Entry e = entries.poll();
        if (reference_pairs.containsKey(e.pair)) {
          score_writer.write((found / (double) correct) + "\t" + (found / (double) total) + "\n");
          found -= reference_pairs.get(e.pair);
        }
        total--;
      }
      
      score_writer.close();
    } catch (IOException e) {
      logger.severe(e.getMessage());
    }
  }
}


class Entry implements Comparable {
  String pair;
  double score;

  public Entry(String p, double s) {
    pair = p;
    score = s;
  }

  @Override
  public int compareTo(Object o) {
    Entry that = (Entry) o;
    return Double.compare(this.score, that.score);
  }
}
