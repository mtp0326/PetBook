var db = require('../models/database.js');
var dbChat = require('../models/chatDB.js');

// Renders the chat page (user's chats, online users, chat invites)
var getChat = function(req, res) {
  if (!req.session.user) {
    return res.redirect("/");
  }
  res.render('chat.ejs', {message: null});
};



// getUserChatrooms
// getOnlineUsers
// routes on chat invites






var routes = { 
  get_chat: getChat,
};

module.exports = routes;
