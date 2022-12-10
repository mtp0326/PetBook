var db = require('../models/database.js');
var chatdb = require('../models/chatDB.js');

// Renders the chat page (user's chats, online users, chat invites)
var getChat = function (req, res) {
	if (!req.session.username) {
		// If not logged in, return to login page
		return res.redirect("/");
	}
	res.render('chat.ejs', { message: null });
};

// Send all online user info to chat page
var getOnlineUsers = function (req, res) {
	var friendsList = [];
	var onlineFriends = [];
	var counter = 0;
	var data2Length = 0;

	// Checks whether all fields are filled; if not, show warning message	
	if (!req.session.username) {
		res.render('login.ejs', { message: "Need to log in" });
	} else {
		db.getFriends(req.session.username, function (err1, data1) {
			if (err1) {
				console.log(err1);
			} else {
				friendsList = data1.map(obj => obj.S);
				//console.log("friend: " + friendsList);
				chatdb.getOnlineUsers(function (err2, data2) {
					if (err2) {
						console.log(err2);
					} else {
						// data2: string set of userIDs	
						data2Length = data2.length;
						console.log(friendsList.includes("hello"));

						data2.forEach(function (r) {
							if (friendsList.includes(r)) {
								var stringifyFriend = {
									S: r
								}
								console.log("print");
								console.log(stringifyFriend);
								onlineFriends.push(stringifyFriend);
							}
							counter++;
							if (data2Length === counter) {
								console.log(onlineFriends);
								res.json(onlineFriends);
							}
						});
					}
				});
			}
		});
	}
};

var addOnlineUser = function (req, res) {
	// Checks whether all fields are filled; if not, show warning message	
	if (!req.session.username) {
		res.render('login.ejs', { message: "Not logged in" });
	} else {
		chatdb.addUserOnline(req.session.username, function (err, data) {
			if (err) { console.log(err); }
		});
	}
}

// Send user's chatroom info to chat page
var getChatRooms = function (req, res) {
	var chatroomList = []

	if (!req.session.username) {
		return res.redirect('/')
	} else {
		//retrieves user's chatroom ids in list of strings
		chatdb.getUserChatroomIDs(req.session.username, function (err1, data1) {
			if (err1) {
				console.log(err1);
			} else {
				console.log(data1);
				// data1: list of all the user's chatrooms ids
				data1.forEach(function (r) {
					chatdb.getChatroom(r.S, function (err2, data2) {
						if (err2) {
							console.log(err2);
						} else {
							// data2: Item with chatID, content, userIDs
							chatroomList.push(data2);
							if (data1.length == chatroomList.length) {
								// list of data2
								res.json(chatroomList);
							}
						}
					})
				});
			}
		});
	}
};

// Add a new instance of chatroom
var addChatRoom = function (req, res) {
	// info needed: timepost
	if (!req.session.username) {
		res.render('login.ejs', { message: "Not logged in" });
	}
	var timepost = new Date().getTime();
	var chatID = req.session.username.concat("-", timepost.toString());

	// Checks whether all fields are filled; if not, show warning message	

	chatdb.addChatroom(req.session.username, chatID, function (err, data) {
		if (err) {
			console.log(err);
		}
			chatdb.addChatIDToUser(req.session.username, chatID, function (err1, data1) {
				if (err1) { console.log(err1); }
			});

	});
}

/*
var deleteChatroom = function(req, res) {
	var chatID = req.body.chatID;
	if (!req.session.username) {
		res.render('login.ejs', {message: "Not logged in"});
	} else {
		chatdb.deleteChatroom(chatID, function(err, data) {
			if (err) {console.log(err);}
		});
	}
}
*/

// Add a new instance of chatroom
var addMessage = function (req, res) {

	// info needed from frontend: chat id, content
	var chatID = req.body.chatID;
	var content = req.body.message;

	var timepost = new Date().getTime();

	//message([timepost, userID, content])
	var message = [timepost, req.session.username, content];

	if (!req.session.username) {
		res.render('login.ejs', { message: "Not logged in" });
	} else {
		chatdb.addMessage(chatID, message, [], function (err, data) {
			if (err) { console.log(err); }
		});
	}
}

var addUserToChatroom = function (req, res) {
	var groupChatID = req.body.chatID;
	if (!req.session.username) {
		res.render('login.ejs', { message: "Not logged in" });
	} else {
		chatdb.addChatIDToUser(req.session.username, groupChatID, function (err, data) {
			if (err) {
				console.log(err);
			} else {
				chatdb.addUserToChat(req.session.username, groupChatID, function (err, data) {
					if (err) { console.log(err) }
				});
			}
		});
	}
}

var deleteUserFromChatroom = function (req, res) {
	var groupChatID = req.body.chatID;
	if (!req.session.username) {
		res.render('login.ejs', { message: "Not logged in" });
	} else {
		chatdb.deleteChatIDFromUser(req.session.username, groupChatID, function (err, data) {
			if (err) {
				console.log(err);
			} else {
				chatdb.deleteUserFromChat(req.session.username, groupChatID, function (err, data) {
					if (err) { console.log(err) }
				});
			}
		});
	}
}

// Deletes session, userID from online db
var logout = function (req, res) {
	if (!req.session.username) {
		res.render('login.ejs', { message: "Not logged in" });
	} else {
		chatdb.deleteUserOnline(req.session.username, function (err, data) {
			if (err) { console.log(err); }
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
	// delete_chatroom: deleteChatroom,
	add_message: addMessage,
	add_user_to_chat: addUserToChatroom,
	delete_user_from_chat: deleteUserFromChatroom,

	log_out: logout,
};

module.exports = routes;