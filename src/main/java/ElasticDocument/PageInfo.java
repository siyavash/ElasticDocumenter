package ElasticDocument;

import javafx.util.Pair;

import java.util.ArrayList;

public class PageInfo {
    private String url;
    private String bodyText;
    private String titleMeta;
    private String descriptionMeta;
    private String keyWordsMeta;
    private String authorMeta;
    private String contentTypeMeta;
    private String title;
    private int numOfInputLinks;
    private ArrayList<Pair<String, Integer>> inputAnchors;
    private double pageRank;

    public String getTitleMeta() {
        return titleMeta;
    }

    public String getDescriptionMeta() {
        return descriptionMeta;
    }

    public String getKeyWordsMeta() {
        return keyWordsMeta;
    }

    public String getAuthorMeta() {
        return authorMeta;
    }

    public String getContentTypeMeta() {
        return contentTypeMeta;
    }

//    public ArrayList<Pair<String, Integer>> getInputAnchors() {
//        return inputAnchors;
//    }

//    public int getPageRank() {
//        return pageRank;
//    }

    public void setNumOfInputLinks(int numOfInputLinks) {
        this.numOfInputLinks = numOfInputLinks;

    }

    public String getBodyText() {
        return bodyText;
    }

    public String getTitle() {
        return title;
    }

    public int getNumOfInputLinks() {

        return numOfInputLinks;
    }

    public void setInputAnchors(ArrayList<Pair<String, Integer>> inputAnchors) {
        this.inputAnchors = inputAnchors;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setBodyText(String bodyText) {
        this.bodyText = bodyText;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setTitleMeta(String titleMeta) {
        this.titleMeta = titleMeta;
    }

    public void setDescriptionMeta(String descriptionMeta) {
        this.descriptionMeta = descriptionMeta;
    }

    public void setKeyWordsMeta(String keyWordsMeta) {
        this.keyWordsMeta = keyWordsMeta;
    }

    public void setAuthorMeta(String authorMeta) {
        this.authorMeta = authorMeta;
    }

    public void setContentTypeMeta(String contentTypeMeta) {
        this.contentTypeMeta = contentTypeMeta;
    }

    public void setPageRank(double pageRank) {
        this.pageRank = pageRank;
    }

    public String getUrl() {
        return url;
    }

    public double getPageRank() {
        return pageRank;
    }
}
