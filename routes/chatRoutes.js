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

    // Checks whether all fields are filled; if not, show warning message	
	if (!req.session.username) {
		res.render('login.ejs', {message: "Need to log in"});
	} else {
		chatdb.getOnlineUsers(function(err, data) {
			if (err) {
				console.log(err);
			} else {
				// data: string set of userIDs	
				res.json(data);
			}
		});	
	}
};



var logout = function(req, res) {
	req.session.destroy();
	res.redirect("/");
}


var routes = { 
    get_chat: getChat,
    get_online_users: getOnlineUsers,
  
    log_out: logout,
};

module.exports = routes;
