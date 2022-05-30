package IPLProjetSQL;
import java.sql.DriverManager;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws Exception {
        //IplProject
        String url = "jdbc:mysql://localhost:3306/Ipl";
        String userName = "root";
        String password = "IronManZone24";
        String query1 = "SELECT\n" +
                "season,count(season) as season_count\n" +
                "FROM IPLMatches\n" +
                "GROUP BY season\n" +
                "ORDER BY season ASC;";
        String query2 = "SELECT\n" +
                "winner,count(winner) \n" +
                "FROM IPLMatches\n" +
                "GROUP BY winner\n" +
                "ORDER BY count(winner) DESC;";
        String query3 = "SELECT \n" +
                "IPLDeliveries.bowling_team,\n" +
                "SUM(IPLDeliveries.extra_runs) AS ConcededRuns \n" +
                "FROM IPLMatches INNER JOIN IPLDeliveries ON IPLMatches.id= IPLDeliveries.match_id \n" +
                "WHERE season =2016 \n" +
                "GROUP BY IPLDeliveries.bowling_team;";
        String query4 = "SELECT \n" +
                "IPLDeliveries.bowler,\n" +
                "SUM(IPLDeliveries.total_runs)/CAST(count(IPLDeliveries.bowler )/6 AS DECIMAL(0)) AS Economy \n" +
                "FROM IPLMatches INNER JOIN IPLDeliveries ON IPLMatches.id= IPLDeliveries.match_id WHERE season =2015 \n" +
                "GROUP BY IPLDeliveries.bowler \n" +
                "ORDER BY Economy;";
        String query5 = "SELECT \n" +
                "IPLDeliveries.batsman,\n" +
                "SUM(batsman_runs)\n" +
                "FROM IPLDeliveries\n" +
                "GROUP BY IPLDeliveries.batsman;";

        findNumberOfMatchesPlayedPerYear(query1, url, userName, password);
        findNumberOfMatchesWonPerTeam(query2, url, userName, password);
        findExtrarunsConcededPerTeamIn2016(query3, url, userName, password);
        findMostEconomicalBowlerIn2015(query4, url, userName, password);
        findToatalRunsScoredByEveryPlayer(query5, url, userName, password);
    }

    private static void findNumberOfMatchesPlayedPerYear(String query1, String url, String userName, String password) throws Exception {

        Map<String, Integer> numberOfMatchesPlayedPerYear = new LinkedHashMap<>();

        Connection con = DriverManager.getConnection(url, userName, password);
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(query1);

        while (rs.next()) {
            if (numberOfMatchesPlayedPerYear.containsKey(rs.getString("season"))) {
                numberOfMatchesPlayedPerYear.put(rs.getString("season"), numberOfMatchesPlayedPerYear.get((rs.getString("season") + rs.getInt("season_count"))));
            } else {
                numberOfMatchesPlayedPerYear.put(rs.getString("season"), rs.getInt("season_count"));
            }
        }
        System.out.println("numberOfMatchesPlayedPerYear");
        System.out.println(numberOfMatchesPlayedPerYear);
        //System.out.println("Number of matches played per year in IPL");
        numberOfMatchesPlayedPerYear.forEach((season, season_count) -> {
            System.out.println(season + " : " + season_count);
        });
    }

    private static void findNumberOfMatchesWonPerTeam(String query2, String url, String uname, String pass) throws SQLException {

        Map<String, Integer> numberOfMatchesWonPerTeamInAllSeasons = new LinkedHashMap<>();
        Connection con = DriverManager.getConnection(url, uname, pass);
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(query2);

        while (rs.next()) {
            if (numberOfMatchesWonPerTeamInAllSeasons.containsKey(rs.getString(1))) {
                numberOfMatchesWonPerTeamInAllSeasons.put(rs.getString(1), numberOfMatchesWonPerTeamInAllSeasons.get(rs.getString(1) + rs.getInt(2)));
            } else {
                numberOfMatchesWonPerTeamInAllSeasons.put(rs.getString(1), rs.getInt(2));
            }
        }
        System.out.println("numberOfMatchesWonPerTeamInAllSeasons");
        numberOfMatchesWonPerTeamInAllSeasons.forEach((team, numberOfWins) -> {
            System.out.println(team + " : " + numberOfWins);
        });
        st.close();
        con.close();
    }

    private static void findExtrarunsConcededPerTeamIn2016(String query3, String url, String uname, String pass) throws SQLException {
        Map<String, Integer> numberOfExtraRunsConcededPerTeamIn2016 = new LinkedHashMap<>();
        Connection con = DriverManager.getConnection(url, uname, pass);
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(query3);

        while (rs.next()) {
            if (numberOfExtraRunsConcededPerTeamIn2016.containsKey(rs.getString(1))) {
                numberOfExtraRunsConcededPerTeamIn2016.put(rs.getString(1), numberOfExtraRunsConcededPerTeamIn2016.get(rs.getString(1) + rs.getInt(2)));
            } else {
                numberOfExtraRunsConcededPerTeamIn2016.put(rs.getString(1), rs.getInt(2));
            }
        }
        System.out.println("numberOfConcededRunsPerTeam");
        numberOfExtraRunsConcededPerTeamIn2016.forEach((team, extraRuns) -> {
            System.out.println(team + " : " + extraRuns);
        });
        st.close();
        con.close();

    }

    private static void findMostEconomicalBowlerIn2015(String query4, String url, String uname, String pass) throws SQLException {
        Map<String, Integer> mostEconomicalBowlerIn2015 = new LinkedHashMap<>();
        Connection con = DriverManager.getConnection(url, uname, pass);
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(query4);

        while (rs.next()) {
            if (mostEconomicalBowlerIn2015.containsKey(rs.getString(1))) {
                mostEconomicalBowlerIn2015.put(rs.getString(1), mostEconomicalBowlerIn2015.get(rs.getString(1) + rs.getInt(2)));
            } else {
                mostEconomicalBowlerIn2015.put(rs.getString(1), rs.getInt(2));
            }
        }
        System.out.println("Most economicaL bowlers in 2015");
        System.out.println(mostEconomicalBowlerIn2015);
        st.close();
        con.close();
    }

    private static void findToatalRunsScoredByEveryPlayer(String query5, String url, String uname, String pass) throws SQLException {
        Map<String, Integer> totalRunsScoredByEveryPlayer = new LinkedHashMap<>();
        Connection con = DriverManager.getConnection(url, uname, pass);
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(query5);

        while (rs.next()) {
            if (totalRunsScoredByEveryPlayer.containsKey(rs.getString(1))) {
                totalRunsScoredByEveryPlayer.put(rs.getString(1), totalRunsScoredByEveryPlayer.get(rs.getString(1) + rs.getInt(2)));
            } else {
                totalRunsScoredByEveryPlayer.put(rs.getString(1), rs.getInt(2));
            }
        }
        System.out.println("players total score over all the ipl seasons");
        totalRunsScoredByEveryPlayer.forEach((key, value) -> {
            System.out.println(key + " : " + value);
        });
        st.close();
        con.close();
    }
}
