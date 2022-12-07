var db = require('../models/database.js');
var chatdb = require('../models/chatDB.js');

// Renders the chat page (user's chats, online users, chat invites)
var getChat = function(req, res) {
    if (!req.session.username) {
	  // If not logged in, return to login page
      return res.redirect("/");
    } 
    res.render('chat.ejs', {message: null});
};

// Send all online user info to chat page
var getOnlineUsers = function(req, res) {
	var friendsList = [];
	var onlineFriends = [];
	
    // Checks whether all fields are filled; if not, show warning message	
	if (!req.session.username) {
		res.render('login.ejs', {message: "Need to log in"});
	} else {
		db.getFriends(req.session.username, function(err1, data1){
    		friendsList = data1.map(obj => obj.S);
    		//console.log("friend: " + friendsList);
  		});
  		
		chatdb.getOnlineUsers(function(err2, data2) {
			if (err2) {
				console.log(err2);
			} else {
				// data2: string set of userIDs				
				data2.forEach(function(r) {
					if (friendsList.includes(r.S)) {
						onlineFriends.push(r.S);
					}
				});	
				res.json(onlineFriends);
			}
		});	
	}
};

var addOnlineUser = function(req, res) {
	// Checks whether all fields are filled; if not, show warning message	
	if (!req.session.username) {
		res.render('login.ejs', {message: "Not logged in"});
	} else {
		chatdb.addUserOnline(req.session.username, function(err, data) {
			if (err) {console.log(err);}
		});
	}
}

// Send user's chatroom info to chat page
var getChatRooms = function(req, res) {
	var chatroomList = []

	if (!req.session.username) {
		res.render('login.ejs', {message: "Need to log in"});
	} else {
		//retrieves user's chatroom ids in list of strings
		chatdb.getUserChatroomIDs(req.session.username, function(err1, data1) {
			if (err1) {
				console.log(err1);
			} else {
				
				// data1: list of all the user's chatrooms ids
				data1.forEach(function(r) {					
					chatdb.getChatroom(r.S, function(err2, data2) {
						if (err2) {
							console.log(err2);
						} else {
							// data2: Item with chatID, content, userIDs
							chatroomList.push(data2);
						}
					})
				});
				// list of data2
				res.json(chatroomList);				
			}
		});
	}
};

// Add a new instance of chatroom
var addChatRoom = function(req, res) {
	// info needed: timestamp
	var timestamp = new Date().getTime();
	
	// Checks whether all fields are filled; if not, show warning message	
	if (!req.session.username) {
		res.render('login.ejs', {message: "Not logged in"});
	} else {
		chatdb.addChatroom(req.session.username, timestamp, [], function(err, data) {
			if (err) {console.log(err);}
		});
	}
}


// Deletes session, userID from online db
var logout = function(req, res) {
	if (!req.session.username) {
		res.render('login.ejs', {message: "Not logged in"});
	} else {
		chatdb.deleteUserOnline(req.session.username, function(err, data) {
			if (err) {console.log(err);}
		});	
	}
	req.session.destroy();
	res.redirect("/");
}


var routes = {
    get_chat: getChat,
    get_online_users: getOnlineUsers,
    get_chatrooms: getChatRooms,
    add_online_user: addOnlineUser,
    add_chatroom: addChatRoom,

    log_out: logout,
};

module.exports = routes;
