/* NOTICE: All materials provided by this project, and materials derived 
 * from the project, are the property of the University of Texas. 
 * Project materials, or those derived from the materials, cannot be placed 
 * into publicly accessible locations on the web. Project materials cannot 
 * be shared with other project teams. Making project materials publicly 
 * accessible, or sharing with other project teams will result in the 
 * failure of the team responsible and any team that uses the shared materials. 
 * Sharing project materials or using shared materials will also result 
 * in the reporting of all team members for academic dishonesty. 
 */
package cs4347.jdbcGame.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import cs4347.jdbcGame.dao.GamesPlayedDAO;
import cs4347.jdbcGame.entity.CreditCard;
import cs4347.jdbcGame.entity.Game;
import cs4347.jdbcGame.entity.GamesPlayed;
import cs4347.jdbcGame.util.DAOException;

public class GamesPlayedDAOImpl implements GamesPlayedDAO
{

	private static final String insertSQL = "INSERT INTO gamesPlayed (player_ID, game_ID, time_finished, score) VALUES (?, ?, ?, ?);";
    @Override
    public GamesPlayed create(Connection connection, GamesPlayed gamesPlayed) throws SQLException, DAOException
    {
    	if (gamesPlayed.getId() != null) {
    		throw new DAOException("Trying to insert Game with NON-NULL ID");
    	}
    	
    	PreparedStatement ps = null;
    	try {
    		ps = connection.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
    		ps.setLong(1, gamesPlayed.getPlayerID());
    		ps.setLong(2, gamesPlayed.getGameID());
    		ps.setDate(3, new java.sql.Date(gamesPlayed.getTimeFinished().getTime()));
    		ps.setInt(4, gamesPlayed.getScore());
    		ps.executeUpdate();
    		
    		ResultSet keyRS = ps.getGeneratedKeys();
    		keyRS.next();
    		int lastKey = keyRS.getInt(1);
    		gamesPlayed.setId((long) lastKey);
    		return gamesPlayed;
    		
    	}
    	finally {
    		if (ps != null && !ps.isClosed()) {
    			ps.close();
    		}
    	}
     
    }

    final static String selectSQL = "SELECT id, player_ID, game_ID, time_finished, score FROM gamesPlayed where id = ?";
    @Override
    public GamesPlayed retrieveID(Connection connection, Long gamePlayedID) throws SQLException, DAOException
    {
    	if(gamePlayedID == null) {
    		throw new DAOException("Trying to retrieve game with NULL ID");
    	}
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(selectSQL);
            ps.setLong(1, gamePlayedID);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                return null;
            }

            GamesPlayed game = extractFromRS(rs);
            return game;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

	public static final String retrievePlayerGameID = "SELECT * FROM GamesPlayed WHERE player_ID = ? AND game_ID = ?";
    @Override
    public List<GamesPlayed> retrieveByPlayerGameID(Connection connection, Long player_ID, Long game_ID)
            throws SQLException, DAOException
    {
    	PreparedStatement ps = null;
    	List<GamesPlayed> gamesList = new ArrayList<GamesPlayed>();
    	try {
    		ps = connection.prepareStatement(retrievePlayerGameID);
    		ps.setLong(1, player_ID);
    		ps.setLong(2, game_ID);
    		ResultSet rs = ps.executeQuery();
    		
    		while (rs.next()) {
    			GamesPlayed games = extractFromRS(rs);
    			gamesList.add(games);
    		}
    		return gamesList;
    	}
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    public static final String retrievePlayerID = "SELECT id, player_ID, game_ID, time_finished, score FROM gamesPlayed where player_ID = ?";
    @Override
    public List<GamesPlayed> retrieveByPlayer(Connection connection, Long player_ID) throws SQLException, DAOException
    {
    	PreparedStatement ps = null;
    	List<GamesPlayed> gamesList = new ArrayList<GamesPlayed>();
    	try {
    		ps = connection.prepareStatement(retrievePlayerID);
    		ps.setLong(1, player_ID);
    		ResultSet rs = ps.executeQuery();
    		
    		while (rs.next()) {
    			GamesPlayed games = extractFromRS(rs);
    			gamesList.add(games);
    		}
    		return gamesList;
    	}
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    public static final String retrieveGameID = "SELECT * FROM GaMesPlaYeD WHERE game_ID = ?";
    @Override
    public List<GamesPlayed> retrieveByGame(Connection connection, Long game_ID) throws SQLException, DAOException
    {

    	List<GamesPlayed> gamesList = new ArrayList<GamesPlayed>();
    	PreparedStatement ps = null;
    	try {
    		ps = connection.prepareStatement(retrieveGameID);
    		ps.setLong(1, game_ID);
    		ResultSet rs = ps.executeQuery();
    		
    		while (rs.next()) {
    			GamesPlayed games = extractFromRS(rs);
    			gamesList.add(games);
    		}
    		return gamesList;
    	}
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    final static String updateSQL = "UPDATE GamesPlayed SET player_ID = ?, game_ID = ?, time_finished = ?, score = ? WHERE id = ?;";
    @Override
    public int update(Connection connection, GamesPlayed gamesPlayed) throws SQLException, DAOException
    {
        Long id = gamesPlayed.getId();
        if (id == null) {
            throw new DAOException("Trying to update Game with NULL ID");
        }

        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(updateSQL);
    		ps.setLong(1, gamesPlayed.getPlayerID());
    		ps.setLong(2, gamesPlayed.getGameID());
    		ps.setDate(3, new java.sql.Date(gamesPlayed.getTimeFinished().getTime()));
    		ps.setInt(4, gamesPlayed.getScore());
    		ps.setLong(5, gamesPlayed.getId());
    		ps.executeUpdate();
    		

            int rows = ps.executeUpdate();
            return rows;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    final static String deleteSQL = "delete from gamesplayed where id = ?;";
    @Override
    public int delete(Connection connection, Long gamePlayedID) throws SQLException, DAOException
    {
        if (gamePlayedID == null) {
            throw new DAOException("Trying to delete Game with NULL ID");
        }

        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(deleteSQL);
            ps.setLong(1, gamePlayedID);

            int rows = ps.executeUpdate();
            return rows;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    final static String countSQL = "select count(*) from GamesPlayed";
    @Override
    public int count(Connection connection) throws SQLException, DAOException
    {
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(countSQL);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                throw new DAOException("No Count Returned");
            }
            int count = rs.getInt(1);
            return count;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }
    private GamesPlayed extractFromRS(ResultSet rs) throws SQLException
    {
        GamesPlayed game = new GamesPlayed();
        game.setId(rs.getLong("id"));
        game.setPlayerID(rs.getLong("player_ID"));
        game.setGameID(rs.getLong("game_ID"));
        game.setTimeFinished(rs.getDate("time_finished"));
        game.setScore(rs.getInt("score"));
        return game;
    }

}