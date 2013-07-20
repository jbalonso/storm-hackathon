package shakeandquake;

import com.google.common.io.Files;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.StringReader;

public class Tweet {

    private String id;
    private String content;
    private DateTime timestamp;

    public Tweet(String id, String content, DateTime timestamp) {
        this.id = id;
        this.content = content;
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public String getContent() {
        return content;
    }

    public int getHourOfDay() {
        return timestamp.getHourOfDay();
    }

    public static Tweet parseTweetXml(final String entryXml) {
        XPathFactory xpathFactory = XPathFactory.newInstance();
        XPath xpath = xpathFactory.newXPath();

        InputSource source = new InputSource(new StringReader(entryXml));

        Tweet tweet = null;
        try {
            Document doc = (Document) xpath.evaluate("/", source, XPathConstants.NODE);

            String id = xpath.evaluate("/entry/id", doc);
            String category = xpath.evaluate("/entry/category", doc);
            String content = xpath.evaluate("/entry/object/content", doc);
            String publishedValue = xpath.evaluate("/entry/published", doc);

            // Ignore any tweets that have been deleted
            if("Note deleted".equalsIgnoreCase(category)) {
                return null;
            }

            DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZoneUTC();
            DateTime publishedTimestamp = fmt.parseDateTime(publishedValue);

            tweet = new Tweet(id, content, publishedTimestamp);
        } catch (Exception e) {
            System.out.println("Error parsing tweet... ignoring it");
        }

        return tweet;
    }

    public static void main(String[] args) throws Exception {
        parseTweetXml(new String(Files.toByteArray(new File("data/spritzer-one.xml"))));
        parseTweetXml(new String(Files.toByteArray(new File("data/spritzer-bad.xml"))));
    }
}
