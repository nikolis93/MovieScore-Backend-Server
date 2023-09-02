package com.mycompany.moviescorebackendserver;


import java.util.ArrayList;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class RTCTools {

    static long lastCon = 0;
    static int period = 700;

    private static Document connect(String url) {
        long elapsed = System.currentTimeMillis() - lastCon;
        if (elapsed < period) {
            try {
                Thread.sleep(period - elapsed);
            } catch (Exception e) {
            }
        }
        lastCon = System.currentTimeMillis();
        try {
            return Jsoup.connect(url).timeout(100 * 1000).get(); //.timeout(30 * 1000).get()
        } catch (Exception e) {
            System.out.println("ERROR: FTEEI TO CONNECTION " + e.getMessage());
            return null;
        }
    }

    public static ArrayList<String> crawlUser(String url) {
        url += "/ratings";
        ArrayList<String> userRatings = new ArrayList<String>(); //edw mpenoun oles oi tainies pou exei kanei rating enas user
        Document doc = null;
        doc = connect(url);
        if (doc == null) {
            System.out.println("ERROR: null doc in crawlUser");
            return null;
        }

        Elements els = doc.getElementsByClass("media-body");//PROSOXI: den pernw oles tis tainies :/

        float userRating;
        String movieName;
        String movieYear;
        String movieUrl;
        String imageUrl;
        System.out.println("els :" + els.size());
        int counter = 0;
        for (Element e : els) {
            userRating = e.getElementsByClass("glyphicon-star").size();
            try {
                String checkForHalfRating = e.getElementsByTag("div").get(3).html();
                if (checkForHalfRating.contains("½")) {//gia na paroume to miso asteri
                    userRating += 0.5;                   
                }
            } catch (Exception exp) {
                System.out.println("ERROR: in halfRating:  " + exp.getMessage());
                return userRatings;
            }

            if (userRating == 0) {
                continue;
            }
            imageUrl = doc.getElementsByClass("bottom_divider").get(counter++).getElementsByTag("img").attr("src");
            movieName = e.getElementsByTag("a").get(1).html();
            try {
                movieYear = e.getElementsByTag("span").get(0).html();
            } catch (Exception ex) {
                System.out.println("ERROR: stin tainia: " + movieName);
                System.out.println(ex.getMessage());
                continue;
            }

            movieUrl = "https://www.rottentomatoes.com" + e.getElementsByTag("a").get(1).attr("href");
            userRatings.add(movieUrl + "-@-" + userRating + "-@-" + movieName + "-@-" + imageUrl);
        }
        System.out.println("Done crawling user " + url);
        return userRatings;
    }
}
