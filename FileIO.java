package yifan;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;

/**
 * Concerns File Input and Output Implementations.
 */

public class FileIO {
    public List<Map<String, String>> parseCran() {

        List<Map<String, String>> cranList = new ArrayList<Map<String, String>>();
        try {
            File file = new File("/home/ec2-user/luceneAssignment/cran.all.1400");
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            Map<String, String> cranDict = new HashMap<String, String>();

            String line;
            String next = ".I";
            int lineNumber = 0;

            String id = "";
            String title = "";
            String authors = "";
            String locations = "";
            String abst = "";
            // Loop through each line
            while ((line = bufferedReader.readLine()) != null) {

                lineNumber++;

                String[] words = line.split("\\s+");
                switch (words[0]) {

                    case ".I":
                        if (next != ".I") printParseError(lineNumber);
                        assert (Integer.parseInt(words[1]) - 1) == cranList.size();
                        if (lineNumber > 1) {

                            cranDict.put("ID", id);
                            cranDict.put("Abstract", abst);
                            cranList.add(cranDict);
                            cranDict = new HashMap<String, String>();
                        }
                        id = words[1];
                        abst = "";
                        next = ".T";
                        break;

                    case ".T":
                        if (next != ".T") printParseError(lineNumber);
                        next = ".A";
                        break;

                    case ".A":
                        if (next != ".A") {

                            if (next == ".I") break;
                            printParseError(lineNumber);
                        }
                        cranDict.put("Title", title);
                        title = "";
                        next = ".B";
                        break;

                    case ".B":
                        if (next != ".B") {

                            if (next == ".I") break;
                            printParseError(lineNumber);
                        }
                        cranDict.put("Authors", authors);
                        authors = "";
                        next = ".W";
                        break;

                    case ".W":
                        if (next != ".W") {

                            if (next == ".I") break;
                            printParseError(lineNumber);
                        }
                        cranDict.put("Locations", locations);
                        locations = "";
                        next = ".I";
                        break;

                    default:
                        switch (next) {
                            case ".A":
                                title += line + " ";
                                break;
                            case ".B":
                                authors += line + " ";
                                break;
                            case ".W":
                                locations += line + " ";
                                break;
                            case ".I":
                                abst += line + " ";
                                break;
                            default:
                                printParseError(lineNumber);
                        }
                }
            }


            cranDict.put("ID", id);
            cranDict.put("Abstract", abst);
            cranList.add(cranDict);

            fileReader.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return cranList;
    }

    private void printParseError(int lineNumber) {

        System.out.println("Parsing unsuccessful from line " + Integer.toString(lineNumber) + ". Please check the document again till this line.");
    }

    public List<Map<String, String>> parseCranQueries() {

        List<Map<String, String>> cranQueryList = new ArrayList<Map<String, String>>();
        try {


            File file = new File("/home/ec2-user/luceneAssignment/cran.qry");
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);


            Map<String, String> cranQueryDict = new HashMap<String, String>();

            String line;
            String next = ".I";
            int lineNumber = 0;

            String id = "";
            int queryNo = 0;
            String query = "";

            while ((line = bufferedReader.readLine()) != null) {

                lineNumber++;

                line = line.replace("?", "");
                String[] words = line.split("\\s+");
                switch (words[0]) {

                    case ".I":
                        if (next != ".I") printParseError(lineNumber);
                        if (lineNumber > 1) {

                            cranQueryDict.put("ID", id);
                            cranQueryDict.put("QueryNo", Integer.toString(queryNo));
                            cranQueryDict.put("Query", query);
                            cranQueryList.add(cranQueryDict);
                            cranQueryDict = new HashMap<String, String>();
                        }
                        id = words[1];
                        queryNo++;
                        query = "";
                        next = ".W";
                        break;

                    case ".W":
                        if (next != ".W") printParseError(lineNumber);
                        next = ".I";
                        break;

                    default:
                        switch (next) {


                            case ".I":
                                query += line + " ";
                                break;

                            default:
                                printParseError(lineNumber);
                        }
                }
            }


            cranQueryDict.put("ID", id);
            cranQueryDict.put("QueryNo", Integer.toString(queryNo));
            cranQueryDict.put("Query", query);
            cranQueryList.add(cranQueryDict);

            fileReader.close();
        } catch (IOException e) {

            e.printStackTrace();
            System.exit(1);
        }
        return cranQueryList;
    }

    public Map<String, List<List<String>>> parseCranRel(String dataDir) {

        Map<String, List<List<String>>> cranRelDict = new HashMap<String, List<List<String>>>();
        try {

            if (!(new File(dataDir).exists() && new File(dataDir).isDirectory()))
                dataDir = "/home/ec2-user/luceneAssignment/";

            System.out.println("Using data stored in " + dataDir);

            File file = new File(dataDir + "/QRelsCorrectedforTRECeval");
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            String line;
            List<String> refFileContent = new ArrayList<String>();

            while ((line = bufferedReader.readLine()) != null) {

                String[] words = line.split("\\s+");
                refFileContent.add(words[0] + " 0 " + words[1] + " " + words[2]);

                List<List<String>> relevancy = cranRelDict.get(words[0]);

                if (relevancy == null) {

                    relevancy = new ArrayList<List<String>>();
                    List<String> relevant = new ArrayList<String>();
                    List<String> irrelevant = new ArrayList<String>();

                    if (Integer.parseInt(words[2]) <= 3)
                        relevant.add(words[1]);
                    else
                        irrelevant.add(words[1]);

                    relevancy.add(relevant);
                    relevancy.add(irrelevant);
                    cranRelDict.put(words[0], relevancy);
                } else {

                    if (Integer.parseInt(words[2]) <= 3)
                        cranRelDict.get(words[0]).get(0).add(words[1]);
                    else
                        cranRelDict.get(words[0]).get(1).add(words[1]);
                }
            }

            fileReader.close();
            Files.write(Paths.get("output/reference.txt"), refFileContent, Charset.forName("UTF-8"));
            System.out.println("Reference file written to output/reference.txt to be used in TREC Eval.");
        } catch (IOException e) {

            e.printStackTrace();
            System.exit(1);
        }
        return cranRelDict;
    }

    private int getMaxCranRelSize(Map<String, List<List<String>>> cranRelDict) {

        int max = -1;
        Iterator<Entry<String, List<List<String>>>> it = cranRelDict.entrySet().iterator();
        while (it.hasNext()) {

            Entry<String, List<List<String>>> pair = (Entry<String, List<List<String>>>) it.next();
            List<List<String>> value = pair.getValue();
            int num = value.get(0).size() + value.get(1).size();
            if (num > max) max = num;
        }
        return max;
    }

    public void deleteDir(File file) {

        File[] contents = file.listFiles();
        if (contents != null) {

            for (File f : contents) {

                deleteDir(f);
            }
        }
        file.delete();
    }

    public static void main(String[] args) throws Exception {
        String INDEX_DIRECTORY1 = "./index_standard";
        String INDEX_DIRECTORY2 = "./index_english";
        int MAX_RESULTS = 10;
        FileIO fio = new FileIO();
        List<Map<String, String>> cran_data = fio.parseCran();
        List<Map<String, String>> cran_queries = fio.parseCranQueries();
        System.out.println("parse cran.all.1400 and cran.query success!");
        Analyzer analyzer = new StandardAnalyzer();
        Analyzer analyzer1 = new EnglishAnalyzer();
        Directory directory = FSDirectory.open(Paths.get(INDEX_DIRECTORY1));
        Directory directory1 = FSDirectory.open(Paths.get(INDEX_DIRECTORY2));
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriterConfig config2 = new IndexWriterConfig(analyzer1);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        config2.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        IndexWriter iwriter = new IndexWriter(directory, config);
        IndexWriter iwriter1 = new IndexWriter(directory1, config2);
        for (Map<String, String> value : cran_data) {
            Document document = new Document();
            document.add(new TextField("Authors", value.get("Authors").toString(), Field.Store.YES));
            document.add(new TextField("Title", value.get("Title").toString(), Field.Store.YES));
            document.add(new TextField("Abstract", value.get("Abstract").toString(), Field.Store.YES));
            document.add(new TextField("ID", value.get("ID").toString(), Field.Store.YES));
            document.add(new TextField("Locations", value.get("Locations").toString(), Field.Store.YES));
            iwriter.addDocument(document);
            iwriter1.addDocument(document);
        }
        iwriter.close();
        directory.close();
        iwriter1.close();
        directory1.close();
        System.out.println("build Standard and WhiteSpace analyzer indexes success!");
        Directory directory_standard = FSDirectory.open(Paths.get(INDEX_DIRECTORY1));
        Directory directory_whitespace = FSDirectory.open(Paths.get(INDEX_DIRECTORY2));
        DirectoryReader ireader = DirectoryReader.open(directory_standard);
        DirectoryReader ireader2 = DirectoryReader.open(directory_whitespace);
        IndexSearcher isearcher = new IndexSearcher(ireader);
        IndexSearcher isearcher2 = new IndexSearcher(ireader2);
        Scanner scanner = new Scanner(System.in);
        System.out.println("please type the metric of similarity you want to use, 1 for TF-IDF and 2 for BM25");
        String similarity = scanner.nextLine();
        switch (similarity) {
            case "1":
                isearcher.setSimilarity(new ClassicSimilarity());
                isearcher2.setSimilarity(new ClassicSimilarity());
                similarity = "TF-IDF";
                break;
            case "2":
                isearcher.setSimilarity(new BM25Similarity());
                isearcher2.setSimilarity(new BM25Similarity());
                similarity = "BM25";
                break;
            default:
                System.out.println("input error!");
                System.exit(1);
        }
        System.out.println("you have chose " + similarity);
        List<String> resFileContent1 = new ArrayList<String>();
        List<String> resFileContent2 = new ArrayList<String>();

        for (int i = 0; i < cran_queries.size(); i++) {
            Map<String, String> cranQuery = cran_queries.get(i);
            MultiFieldQueryParser queryParser = new MultiFieldQueryParser(
                    new String[]{"Title", "Locations", "Authors", "Abstract"},
                    analyzer);
            Query query = queryParser.parse(cranQuery.get("Query"));
            Map<String, List<String>> resultDict = new HashMap<String, List<String>>();
            Map<String, List<String>> resultDict2 = new HashMap<String, List<String>>();
            Evaluator evaluator = new Evaluator();
            TopDocs topDocs = isearcher.search(query, MAX_RESULTS);
            TopDocs topDocs2 = isearcher2.search(query, MAX_RESULTS);
            ScoreDoc[] hits = topDocs.scoreDocs;
            ScoreDoc[] hits2 = topDocs2.scoreDocs;
            List<String> resultList = new ArrayList<String>();
            List<String> resultList2 = new ArrayList<String>();
            for (int j = 0; j < hits2.length; j++) {
                int docId = hits2[j].doc;
                Document doc = isearcher2.doc(docId);
                resultList2.add(doc.get("ID"));
                resFileContent2.add(cranQuery.get("QueryNo") + " 0 " + doc.get("ID") + " 0 " + hits2[j].score + " STANDARD\n");
            }
            resultDict2.put(Integer.toString(i + 1), resultList2);
            for (int j = 0; j < hits.length; j++) {
                int docId = hits[j].doc;
                Document doc = isearcher.doc(docId);
                resultList.add(doc.get("ID"));
                resFileContent1.add(cranQuery.get("QueryNo") + " 0 " + doc.get("ID") + " 0 " + hits[j].score + " STANDARD\n");
            }
            resultDict.put(Integer.toString(i + 1), resultList);
        }
        File outputDir = new File("output");
        if (!outputDir.exists()) outputDir.mkdir();
        Files.write(Paths.get("output/results_standard.txt"), resFileContent1, Charset.forName("UTF-8"));
        Files.write(Paths.get("output/results_english.txt"), resFileContent2, Charset.forName("UTF-8"));
        System.out.println("Both Results based on Standard Anlyzer and English Analyzer have written to output/ to be used in TREC Eval.");
        ireader.close();
        directory1.close();
        System.out.println("now please follow the tips in README and use trec_eval to see the score of the model");
    }
}

class Evaluator {
    Map<String, Double> calculateMetrics(Map<String, List<String>> resultDict, Map<String, List<List<String>>> cranRelDict) {
        Map<String, Double> metrics = new HashMap<String, Double>();
        List<Double> avgPrecisionList = new ArrayList<Double>();
        List<Double> recallList = new ArrayList<Double>();
        Iterator<Entry<String, List<String>>> resultEntry = resultDict.entrySet().iterator();
        while (resultEntry.hasNext()) {
            Entry<String, List<String>> resultPair = (Entry<String, List<String>>) resultEntry.next();
            String resultID = resultPair.getKey();
            List<String> resultList = resultPair.getValue();
            List<List<String>> cranRelList = cranRelDict.get(resultID);
            List<String> cranRelRelevantList = cranRelList.get(0);
            List<Double> precisionList = new ArrayList<Double>();
            int docCount = 0;
            double recall = 0.0;
            for (int i = 0; i < resultList.size(); i++) {
                String docID = resultList.get(i);
                if (cranRelRelevantList.contains(docID)) {
                    docCount++;
                    precisionList.add((double) docCount / (i + 1));
                    recall = docCount / cranRelRelevantList.size();
                }
            }
            double avgPrecision = 0.0;
            if (precisionList.size() > 0) {

                for (int i = 0; i < precisionList.size(); i++) avgPrecision += precisionList.get(i);
                avgPrecision /= precisionList.size();
            }

            avgPrecisionList.add(avgPrecision);
            recallList.add(recall);
        }
        double map = 0.0;
        for (int i = 0; i < avgPrecisionList.size(); i++) map += avgPrecisionList.get(i);
        map /= avgPrecisionList.size();
        double meanRecall = 0.0;
        for (int i = 0; i < recallList.size(); i++) meanRecall += recallList.get(i);
        meanRecall /= recallList.size();
        metrics.put("MAP", map);
        metrics.put("Mean Recall", meanRecall);
        return metrics;
    }
}
