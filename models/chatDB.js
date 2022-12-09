var AWS = require('aws-sdk');
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();
//var docClient = new AWS.DynamoDB.DocumentClient();

// Gets a list of all the user's chatrooms ids
var myDB_getUserChatrooms = function(username, callback) {
  var params = {
      TableName: "users",
      Key: {"username" : {S: username}},
  };

  db.getItem(params, function(err, data) {
    if (err) {
      console.log("Error", err);
    } else {
      callback(null, data.Item.chatID.SS);
    }
  });
}

// Adds a new chatID to user db
var myDB_addchatIDToUser = function(username, chatID, callback) {	
  var newChatID = {S: chatID};
  var params = {
      TableName: "users",
      Key: {"username" : {S: username}},
	  UpdateExpression: "ADD chatID :newChatID",
	  ExpressionAttributeValues : {
	    ":newChatID": newChatID
	  },
  };
  db.updateItem(params, function(err, data) {
	  if (err) {
	    console.log("Error", err);
	  }
  });
}

// Deletes a chatID from user db
var myDB_deletechatIDFromUser = function(username, chatID, callback) {
  var deleteChatID = {S: chatID};
  var params = {
      TableName: "users",
      Key: {"username" : {S: username}},
	  UpdateExpression: "DELETE chatID :deleteChatID",
	  ExpressionAttributeValues : {
	    ":deleteChatID": deleteChatID
	  },
  };
  db.updateItem(params, function(err, data) {
	  if (err) {
	    console.log("Error", err);
	  }
  });
}

// Gets a list of the online users
var myDB_getOnlineUsers = function(callback) {
  var params = {
      TableName: "online",
      Key: {"users" : {S: "online"}},
  };

  db.getItem(params, function(err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Item.userIDs.SS);
    }
  });
}

// Gets the chatroom info with a given chatID (= "userID created time")
var myDB_getChatroom = function(chatID, callback) {
	var params = {
        TableName: "chatrooms",
        Key: {
			"chatID" : {S: chatID},
		},
	}
	
	db.getItem(params, function(err, data) {
        if (err) {
          callback(err, null);
        } else if (chatID.length == 0) {
          callback("chatID cannot be empty", null);
        } else {
            console.log("get item success");
            console.log(data.Item);
          //data.Item - has chatID, content, userIDs
          callback(null, data.Item);
        }
    });
}

// Adds a new chatroom with given info
var myDB_addChatroom = function(userID, chatID, callback) {
	//otherUserIDs: set of strings
	var params = {
		TableName: "chatrooms",
		Item: {
		  'chatID' : {S: chatID},
		  'userIDs' : {SS: {S: userID}},
		},
	}
	db.putItem(params, function(err, data) {
	    if (err) {
	      console.log("Error", err);
	    } 
	});
}

var myDB_deleteChatroom = function(chatID, callback) {
	var params = {
		TableName: "chatrooms",
		Key: {
			"chatID" : {S: chatID},
		},
	}
	db.deleteItem(params, function(err, data) {
	    if (err) {
	      console.log("Error", err);
	    } 
	});
}

// Adds a new message to the chatroom
//newMessage: [timepost, userID, content]
var myDB_addMessage = function(chatID, newMessage, callback) {
	var params = {
		TableName: "chatrooms",
		Key: {
	      "chatID" : {S: chatID},
		},
	    UpdateExpression: "SET #c = list_append(#c, :new)",
	    ExpressionAttributeNames: {
	      "#c": "content"
	    },
	    ExpressionAttributeValues : {
	      ":new": newMessage
	    },
	}
	db.updateItem(params, function(err, data) {
	    if (err) {
	      console.log("Error", err);
	    }
	});
}

// When a user accepts a group chat invite, add the user to the groupchat and create the chatroom on the user's chat list
var myDB_addUserToChat = function(newUserID, groupChatID, callback) {
	var newUserIDSet = {S: newUserID};
	var params = {
		TableName: "chatrooms",
		Key: {"chatID" : {S: groupChatID}},
		UpdateExpression: "ADD userIDs :newUserID",
	    ExpressionAttributeValues : {
	      ":newUserID": newUserIDSet
	    },
	}
	db.updateItem(params, function(err, data) {
	    if (err) {
	      console.log("Error", err);
	    }
	});
}

// When a user accepts a group chat invite, add the user to the groupchat and create the chatroom on the user's chat list
var myDB_deleteUserFromChat = function(deleteUserID, groupChatID, callback) {
	var deleteUserIDSet = {S: deleteUserID};
	var params = {
		TableName: "chatrooms",
		Key: {"chatID" : {S: groupChatID}},
		UpdateExpression: "DELETE userIDs :deleteUserID",
	    ExpressionAttributeValues : {
	      ":deleteUserID": deleteUserIDSet
	    },
	}
	db.updateItem(params, function(err, data) {
	    if (err) {
	      console.log("Error", err);
	    }
	});
}

// Adds a logged-in user to online DB
var myDB_addOnline = function(newUserID, callback) {
	var newUserIDSet = {S: newUserID};
	var params = {
		TableName: "online",
		Key: {
	      "users" : {S: "online"},
		},
	    UpdateExpression: "ADD #c :new",
	    ExpressionAttributeNames: {
	      "#c": "userIDs"
	    },
	    ExpressionAttributeValues : {
	      ":new": newUserIDSet
	    },
	}
	db.updateItem(params, function(err, data) {
	    if (err) {
	      console.log("Error", err);
	    }
	});
}

// Deletes a logged-out user from online DB
var myDB_deleteOnline = function(deleteUserID, callback) {
	var deleteUserIDSet = {S: deleteUserID};
	var params = {
		TableName: "online",
		Key: {
	      "users" : {S: "online"},
		},
	    UpdateExpression: "DELETE userIDs :new",
	    ExpressionAttributeValues : {
	      ":d": deleteUserIDSet
	    },
	}
	db.updateItem(params, function(err, data) {
	    if (err) {
	      console.log("Error", err);
	    }
	});
}

var chatDB = { 
  getUserChatroomIDs : myDB_getUserChatrooms,
  
  addChatIDToUser : myDB_addchatIDToUser,
  deleteChatIDFromUser : myDB_deletechatIDFromUser,
  
  getOnlineUsers : myDB_getOnlineUsers,
  getChatroom : myDB_getChatroom,
  
  addChatroom : myDB_addChatroom,
  deleteChatroom : myDB_deleteChatroom,

  addMessage : myDB_addMessage,
  
  addUserToChat : myDB_addUserToChat,
  deleteUserFromChat : myDB_deleteUserFromChat,
  
  addUserOnline : myDB_addOnline,
  deleteUserOnline : myDB_deleteOnline,
};

module.exports = chatDB;