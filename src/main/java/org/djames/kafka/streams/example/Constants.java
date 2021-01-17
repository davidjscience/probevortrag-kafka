package org.djames.kafka.streams.example;

/**
 * Created by gwen on 1/28/17.
 */
public class Constants {

    public static final String BROKER = "localhost:9092";

    public static final String USER_TOPIC = "user";
    public static final String PAGE_VIEW_TOPIC = "pageview";
    public static final String PAGE_VIEW_FILTERED_TOPIC = "pageview-filtered";
    public static final String PAGE_VIEW_DURATION_TOPIC = "pageview-duration";
    public static final String PAGE_VIEW_ENRICHED_TOPIC = "pageview-enriched";
    public static final String PAGE_TOPIC = "pages" ;
    public static final String PAGE_VIEW_CATEGORY_SUM_DURATION_TOPIC = "pageview-category-count";
}
