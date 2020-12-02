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
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import cs4347.jdbcGame.dao.PlayerDAO;
import cs4347.jdbcGame.entity.Player;
import cs4347.jdbcGame.util.DAOException;

public class PlayerDAOImpl implements PlayerDAO
{

    private static final String insertSQL = "INSERT INTO player (first_name, last_name, join_date, email) VALUES (?, ?, ?, ?);";

    @Override
    public Player create(Connection connection, Player player) throws SQLException, DAOException
    {
        if (player.getId() != null) {
            throw new DAOException("Trying to insert Player with NON-NULL ID");
        }

        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, player.getFirstName());
            ps.setString(2, player.getLastName());
            ps.setDate(3, new java.sql.Date(player.getJoinDate().getTime()));
            ps.setString(4, player.getEmail());
            ps.executeUpdate();

            // Copy the assigned ID to the customer instance.
            ResultSet keyRS = ps.getGeneratedKeys();
            keyRS.next();
            int lastKey = keyRS.getInt(1);
            player.setId((long) lastKey);
            return player;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    
    
    @Override
    public Player retrieve(Connection connection, Long playerID) throws SQLException, DAOException
    {
        final String retrieveSQL = "SELECT * FROM player WHERE id = ?;";
    	PreparedStatement ps = null;
    	try {
    		ps = connection.prepareStatement(retrieveSQL);
        	ps.setLong(1, playerID);
        	ResultSet rs = ps.executeQuery();
    		
        	if(!rs.next()) {
        		return null;
        	}
        	
        	//Build a new object based on the result
        	Player player = new Player();
        	player.setId(rs.getLong("id"));
        	player.setFirstName(rs.getString("first_name"));
        	player.setLastName(rs.getString("last_name"));
        	player.setJoinDate(rs.getDate("join_date"));
        	player.setEmail(rs.getString("email"));
        	
        	return player;
    	}
    	finally {
    		if(ps != null && !ps.isClosed()) {
    			ps.close();
    		}
    	}
    }

    @Override
    public int update(Connection connection, Player player) throws SQLException, DAOException
    {
    	final String updateSQL = "UPDATE player SET first_name = ?, last_name = ?, join_date = ?, email = ? WHERE id = ?;";
    	
    	PreparedStatement ps = null;
    	try {
    		ps = connection.prepareStatement(updateSQL);
    		ps.setString(1, player.getFirstName());
    		ps.setString(2, player.getLastName());
    		ps.setDate(3, new java.sql.Date(player.getJoinDate().getTime()));
    		ps.setString(4, player.getEmail());
    		ps.setLong(5, player.getId());
        	
        	int count = ps.executeUpdate();
        	
        	/*
        	if(count != 1) {
        		throw new DAOException("Player ID not found");
        	}
        	*/
        	
        	return count;
    	}
    	finally {
    		if(ps != null && !ps.isClosed()) {
    			ps.close();
    		}
    	}
    }

    @Override
    public int delete(Connection connection, Long playerID) throws SQLException, DAOException
    {
        final String deleteSQL = "DELETE FROM player WHERE id = ?";
        
        PreparedStatement ps = null;
        try {
        	ps = connection.prepareStatement(deleteSQL);
        	ps.setLong(1, playerID);
        	
        	int count = ps.executeUpdate();
        	
        	/*
        	if(count != 1) {
        		throw new DAOException("Player ID not found");
        	}
        	*/
        	
        	return count;
        }
        
        finally {
    		if(ps != null && !ps.isClosed()) {
    			ps.close();
    		}
    	}
    }

    
    @Override
    public int count(Connection connection) throws SQLException, DAOException
    {
    	final String countSQL = "SELECT COUNT(*) FROM player;";
    	
    	PreparedStatement ps = null;
    	try {
    		ps = connection.prepareStatement(countSQL);
        	ResultSet rs = ps.executeQuery();	
        	rs.next();
        	
        	/*
        	if(!rs.next()) {
        		throw new DAOException("Unknown exception");
        	}
        	*/
        	
        	return rs.getInt("COUNT(*)");
    	}
    	finally {
    		if(ps != null && !ps.isClosed()) {
    			ps.close();
    		}
    	}    	
    }

    @Override
    public List<Player> retrieveByJoinDate(Connection connection, Date start, Date end)
            throws SQLException, DAOException
    {
    	//Get all the players that joined during a date range
    	final String retrieveString = "SELECT * FROM player WHERE join_date > ? AND join_date < ?";
    	
    	PreparedStatement ps = null;
    	List<Player> playerList = null;
    	try {
    		ps = connection.prepareStatement(retrieveString);
    		ps.setDate(1, new java.sql.Date(start.getTime()));
    		ps.setDate(2, new java.sql.Date(end.getTime()));
    		ResultSet rs = ps.executeQuery();

    		//Iterate through all the results
    		while(rs.next()) {
    			if(playerList == null) {
    				playerList = new LinkedList<Player>();
    			}
    			
    			Player player = new Player();
            	player.setId(rs.getLong("id"));
            	player.setFirstName(rs.getString("first_name"));
            	player.setLastName(rs.getString("last_name"));
            	player.setJoinDate(rs.getDate("join_date"));
            	player.setEmail(rs.getString("email"));
            	
            	playerList.add(player);
    		}
    		
    		return playerList;
    	}
    	finally {
    		if(ps != null && !ps.isClosed()) {
    			ps.close();
    		}
    	}
    }
}
