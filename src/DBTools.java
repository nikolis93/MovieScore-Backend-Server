package com.mycompany.moviescorebackendserver;


import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DBTools {

    static Connection con = null;
    static Statement stmt = null;

    private static void initDB() {
        if (con == null) {
            try {//kanw prwta to connection 
                Class.forName("com.mysql.jdbc.Driver");
                con = DriverManager.getConnection("jdbc:mysql://localhost:3306/newrtdb?useSSL=false&autoReconnect=true", "root", "");
                stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            } catch (SQLException ex) {
                System.out.println("ERROR: in initDB " + ex.getMessage());
            } catch (ClassNotFoundException ex) {
                System.out.println("ERROR: in initDB sto 2ro catch ");
                Logger.getLogger(DBTools.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static ArrayList<String> searchForAMovie(String mName) {
        initDB();
        ArrayList<String> movies = new ArrayList<String>();
        String movieUrl = "";
        String movieName = "";
        String imageUrl = "";
        String line = "";
        if (mName != null) {
            try {
                System.out.println("SELECT movieUrl, movieName, imageUrl FROM movieinfo where  movieName LIKE '%" + mName + "%'");

                 ResultSet rs = stmt.executeQuery("SELECT movieUrl, movieName, imageUrl FROM movieinfo where  movieName LIKE '%" + mName + "%'" + "ORDER BY" +   "  CASE" +"    WHEN movieName LIKE '"+mName+"%' THEN 1" + " WHEN movieName LIKE '%"+mName+"' THEN 3" +"    ELSE 2" +"  END");  
              
                 int counter = 0;
                 while (rs.next()) {
                    movieUrl = rs.getString("movieUrl");
                    movieName = rs.getString("movieName");
                    imageUrl = rs.getString("imageUrl");
                    line = movieUrl + "-@-" + movieName + "-@-" + imageUrl;
                    movies.add(line);
                    if(++counter>=50) {System.out.println("counter: " + counter);break;}
                }
                rs.close();

            } catch (SQLException e) {
                System.out.println("ERROR: in searchForAMovie" + e.getMessage());
                e.printStackTrace();
            }
        }else{
            System.out.println("movie name is null");
        }
        
        return movies;
    }

    public static int randomWithRange(int min, int max) {
        int range = Math.abs(max - min) + 1;
        return (int) (Math.random() * range) + (min <= max ? min : max);  
    }

    public static ArrayList<String> getRandomMoviesFromMovieInfo() {
        initDB();
        ArrayList<String> ar = new ArrayList<String>();
        ArrayList<String> movieInfos = new ArrayList<String>();
        String movieUrl = "";
        String movieName = "";
        String imageUrl = "";
        String line = "";
        try {
            ResultSet rs = stmt.executeQuery("SELECT movieinfo.movieUrl, movieinfo.movieName, movieinfo.imageUrl FROM movieinfo INNER JOIN maxpage ON movieinfo.movieUrl=maxpage.movieUrl where maxpage.maxpg >= 40");
            while (rs.next()) {
                movieUrl = rs.getString("movieUrl");
                movieName = rs.getString("movieName");
                imageUrl = rs.getString("imageUrl");
                line = movieUrl + "-@-" + movieName + "-@-" + imageUrl;
                movieInfos.add(line);
            }
            rs.close();
        } catch (SQLException ex) {
            System.out.println("ERROR: in getRandomMoviesFromMovieInfo " + ex.getMessage());
            Logger.getLogger(DBTools.class.getName()).log(Level.SEVERE, null, ex);
        }
        boolean alreadyInAr = false;
        for (int i = 0; i < 50; i++) {
            line = movieInfos.get(randomWithRange(0, (movieInfos.size()-1)));
            for (String s : ar) {
                if (line.equalsIgnoreCase(s)) {
                    alreadyInAr = true;
                }
            }
            if (!alreadyInAr) {
                ar.add(line);
            }
            alreadyInAr = false;
        }
        return ar;
    }

    public static int tableSize(String table) {
        initDB();
        ResultSet rs;
        try {
            rs = stmt.executeQuery("select count(*) from " + table);
            rs.next();
            return Integer.parseInt(rs.getString("count(*)"));
        } catch (SQLException ex) {
            System.out.println("ERROR: in tableIsEmpty " + ex.getMessage());
            Logger.getLogger(DBTools.class.getName()).log(Level.SEVERE, null, ex);
        }
        return 0;
    }

    public static boolean tableIsEmpty(String table) {
        if (tableSize(table) == 0) {
            return true;
        }
        return false;
    }

    public static boolean deleteFromTable(String url, String field, String table) {
        initDB();
        try {
            stmt.executeUpdate("DELETE from " + table + " WHERE " + field + " = '" + url + "'");
        } catch (SQLException ex) {
            System.out.println("ERROR: in deleteFromTable " + ex.getMessage());
            Logger.getLogger(DBTools.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    public static boolean deleteFromTableP1(String url, String table) {
        initDB();
        try {
            String field = "userId";
            if (table.startsWith("movies")) {
                field = "movieUrl";
            }
            stmt.executeUpdate("DELETE from " + table + " WHERE " + field + " = '" + url + "'");
        } catch (SQLException ex) {
            System.out.println("ERROR: in deleteFromTable " + ex.getMessage());
            Logger.getLogger(DBTools.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    public static void insertToTable(String url, String table) {
        initDB();
        if (!url.isEmpty()) {//an to movieurl dn einai keno ta vazw stin vasi
            try {
                stmt.executeUpdate("INSERT into " + table + " VALUES ('" + url + "')");
            } catch (MySQLIntegrityConstraintViolationException ex) {
                return;
            } catch (SQLException ex) {
                System.out.println("ERROR: in insertToTable " + table + " " + ex.getMessage());
                Logger.getLogger(DBTools.class.getName()).log(Level.SEVERE, null, ex);
            }

        }
    }

    public static boolean tableContains(String url, String table)//checkarw an to url mias tainias iparxei sto table moviesvisited
    {
        initDB();
        try {
            String field = "userId";
            if (table.startsWith("movies")) {
                field = "movieUrl";
            }
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + table + " where " + field + "='" + url + "'");
            return rs.next();
        } catch (SQLException e) {
            System.out.println("ERROR: in checkCrawled" + e.getMessage());
            e.printStackTrace();
        }
        return false;
    }

    public static ArrayList<String> getTheContentsOfTableP1(String table) {
        initDB();
        ArrayList<String> movieUrls = new ArrayList<String>();
        try {
            String field = "userId";
            if (table.startsWith("movies")) {
                field = "movieUrl";
            }
            ResultSet rs = stmt.executeQuery("select " + field + " from " + table);
            while (rs.next()) {
                movieUrls.add(rs.getString(field));
            }
        } catch (SQLException ex) {
            System.out.println("ERROR: in getTheContentsOfmovies2visit " + ex.getMessage());
            Logger.getLogger(DBTools.class.getName()).log(Level.SEVERE, null, ex);
        }
        return movieUrls;
    }

    public static HashMap<String, Integer> getMaxPages() {
        HashMap<String, Integer> res = new HashMap<String, Integer>();
        try {
            ResultSet rs = stmt.executeQuery("select * from maxpage");
            while (rs.next()) {
                res.put(rs.getString("movieUrl"), rs.getInt("maxpg"));
            }
        } catch (SQLException ex) {
            System.out.println("ERROR: in getTheContentsOfmovies2visit " + ex.getMessage());
            Logger.getLogger(DBTools.class.getName()).log(Level.SEVERE, null, ex);
        }
        return res;
    }

    public static ArrayList<String> getTheContentsOfTable(String field, String table) {
        initDB();
        ArrayList<String> movieUrls = new ArrayList<String>();
        try {
            ResultSet rs = stmt.executeQuery("select " + field + " from " + table);
            while (rs.next()) {
                movieUrls.add(rs.getString(field));
            }
        } catch (SQLException ex) {
            System.out.println("ERROR: in getTheContentsOfmovies2visit " + ex.getMessage());
            Logger.getLogger(DBTools.class.getName()).log(Level.SEVERE, null, ex);
        }
        return movieUrls;
    }

    public static ArrayList<String> unionTables(String table1, String table2, String field) {
        initDB();
        ArrayList<String> movieUrls = new ArrayList<String>();
        try {

            ResultSet rs = stmt.executeQuery("SELECT " + field + " FROM " + table1 + " UNION " + "SELECT " + field + " FROM " + table2);
            while (rs.next()) {
                movieUrls.add(rs.getString(field));
            }
        } catch (SQLException ex) {
            System.out.println("ERROR: in getTheContentsOfmovies2visit " + ex.getMessage());
            Logger.getLogger(DBTools.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("The union returned:" + movieUrls.size() + " unique records");
        return movieUrls;
    }

    public static HashMap<String, Integer> getTheContentsOfRatingsGrouped() {
        initDB();
        HashMap<String, Integer> users = new HashMap<String, Integer>();
        String userId = "";
        int countDB = 0;
        int count = 0;//ta count gia na dw posoi exoun mono 1 rating
        try {
            ResultSet rs = stmt.executeQuery("select userId,count(*) from ratings group by userId");
            while (rs.next()) {
                userId = rs.getString("userId");
                countDB = rs.getInt("count(*)");
                System.out.println("userId: " + userId + " count(*): " + countDB);
                users.put(userId, countDB);
                if (countDB == 1) {
                    count++;
                }
            }
        } catch (SQLException ex) {
            System.out.println("ERROR: in getTheContentsOfRatingsGrouped " + ex.getMessage());
            Logger.getLogger(DBTools.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println(count * 1.0 / users.size());
        return users;
    }

    public static void moviesWithLowPages() {
        initDB();
        int count = 0;
        int all = 0;
        try {
            int pages = 0;
            String mName = "";

            ResultSet rs = stmt.executeQuery("SELECT * from maxpage");
            while (rs.next()) {
                pages = rs.getInt("maxpg");
                if (pages <= 3) {
                    count++;
                }
                all++;
            }
        } catch (SQLException ex) {
            System.out.println("ERROR: in getTheContentsOfmovies2visit " + ex.getMessage());
            Logger.getLogger(DBTools.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("There are " + count * 1.0 / all + " movies with less then 4 pages");
    }

    public static void dumpMovieInfo() {
        initDB();
        try {
            ResultSet rs = stmt.executeQuery("select * from movieinfo");
            FileWriter writer = new FileWriter("movieinfo.txt");
            while (rs.next()) {
                String mUrl = rs.getString("movieUrl");               
                writer.write(mUrl + "\n");
            }
            writer.flush();
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getMovieUrls(String movieName) {
        initDB();
        String url = "";
        try {
            ResultSet rs = stmt.executeQuery("select movieUrl from movieinfo where movieName = '" + movieName + "'");
            while (rs.next()) {
                url = rs.getString("movieUrl");
                System.out.println("to url tis tianias " + movieName + " einai: " + url);
            }
        } catch (SQLException ex) {
            System.out.println("ERROR: in getTheContentsOfmovies2visit " + ex.getMessage());
            Logger.getLogger(DBTools.class.getName()).log(Level.SEVERE, null, ex);
        }
        return url;
    }

    public static ResultSet getMovieInfo(String movieUrl) {
        initDB();
        ResultSet rs = null;
        try {
            rs = stmt.executeQuery("select movieinfo.* , maxpage.maxpg from movieinfo INNER JOIN maxpage ON movieinfo.movieUrl=maxpage.movieUrl where movieinfo.movieUrl  = '" + movieUrl + "'");
        } catch (SQLException ex) {
            System.out.println("ERROR: in getMovieInfo " + ex.getMessage());
            Logger.getLogger(DBTools.class.getName()).log(Level.SEVERE, null, ex);
        }
        return rs;
    }

}
