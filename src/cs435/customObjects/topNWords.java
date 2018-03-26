package cs435.customObjects;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

public class topNWords {

    private Set <Double> sortedSet = new TreeSet<>(Comparator.naturalOrder());

    public topNWords(){
        sortedSet = new TreeSet<>(Comparator.naturalOrder());
    }

    public void add(double wordValue){
        sortedSet.add(wordValue);
    }

    public double getSentencetfidf(int topN){
        double tfidf = 0;
        Iterator<Double> list = sortedSet.iterator();
        for(int i = 0; i < topN; i++){
            if(list.hasNext()) {
                tfidf += list.next();
            }
        }
        return tfidf;
    }

    public double getTopK(int k){
        double tfidf = 0;
        Iterator<Double> list = sortedSet.iterator();
        for(int i = 0; i < k; i++){
            if(list.hasNext()) {
                tfidf = list.next();
            }
        }
        return tfidf;
    }


}
