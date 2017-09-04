package ElasticDocument;

import java.util.ArrayList;

public class PageInfo
{
    private String url;
    private String bodyText;
    private String titleMeta;
    private String descriptionMeta;
    private String keyWordsMeta;
    private String authorMeta;
    private String contentTypeMeta;
    private String title;
    private int numOfInputLinks;
    private ArrayList<String> inputAnchors; // TODO: 9/4/17

    public void setNumOfInputLinks(int numOfInputLinks) {
        this.numOfInputLinks = numOfInputLinks;
    }

    public void setInputAnchors(ArrayList<String> inputAnchors) {
        this.inputAnchors = inputAnchors;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    public void setBodyText(String bodyText)
    {
        this.bodyText = bodyText;
    }

    public void setTitle(String title)
    {
        this.title = title;
    }

    public void setTitleMeta(String titleMeta)
    {
        this.titleMeta = titleMeta;
    }

    public void setDescriptionMeta(String descriptionMeta)
    {
        this.descriptionMeta = descriptionMeta;
    }

    public void setKeyWordsMeta(String keyWordsMeta)
    {
        this.keyWordsMeta = keyWordsMeta;
    }

    public void setAuthorMeta(String authorMeta)
    {
        this.authorMeta = authorMeta;
    }

    public void setContentTypeMeta(String contentTypeMeta)
    {
        this.contentTypeMeta = contentTypeMeta;
    }

    public String getUrl() {
        return url;
    }
}
