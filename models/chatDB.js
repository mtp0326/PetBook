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
      console.log("Error" + err);
    } else {
	  if(data.Item.chatID == undefined) {
        var empty = [];
        callback(null, empty);
      } else {
        callback(null, data.Item.chatID.SS);
      }
    }
  });
}

// Adds a new chatID to user db
var myDB_addchatIDToUser = function(username, chatID, callback) {
  var newChatID = {SS: [chatID]};
  var params = {
      TableName: "users",
      Key: {"username" : {S: username}},
	  UpdateExpression: "ADD chatID :n",
	  ExpressionAttributeValues : {
	    ":n": newChatID
	  }
  };
  db.updateItem(params, function(err, data) {
	  if (err) {
	    console.log("Error", err);
	  }
	  callback(err, "success");
  });
}

// Deletes a chatID from user db
var myDB_deletechatIDFromUser = function(username, chatID, callback) {
  var deleteChatID = {SS: [chatID]};
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
	  callback(err, data);
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
		if(data.Item.userIDs == undefined) {
			var empty = [];
			callback(null, empty);
		  } else {
			callback(null, data.Item.userIDs.SS);
		  }
      
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
		  'userIDs' : {SS: [userID]},
		  'content' : {L: [] }
		}
	}
	db.putItem(params, function(err, data) {
	    if (err) {
	      console.log("Error", err);
	    }
		callback(err, "success");
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
		callback(err, data);
	});
}

// Adds a new message to the chatroom
//newMessage: [timepost, userID, content]
var myDB_addMessage = function(chatID, newMessage, callback) {
	var messageList = [];
	for(let i = 0; i < 2; i++) {
		var stringifyMessage = {S : newMessage[i]};
		messageList.push(stringifyMessage);
	}

	var listListMessage = {L : [{L : messageList}]}
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
	      ":new": listListMessage
	    }
	}
	db.updateItem(params, function(err, data) {
	    if (err) {
	      console.log("Error", err);
	    }
		// callback(err, data);
	});
}

// When a user accepts a group chat invite, add the user to the groupchat and create the chatroom on the user's chat list
var myDB_addUserToChat = function(newUserID, groupChatID, callback) {
	var newUserIDSet = {SS: [newUserID]};
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
		callback(err, data);
	});
}

// When a user accepts a group chat invite, add the user to the groupchat and create the chatroom on the user's chat list
var myDB_deleteUserFromChat = function(deleteUserID, groupChatID, callback) {
	var deleteUserIDSet = {SS: [deleteUserID]};
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
		callback(err, data);
	});
}





// Deletes a logged-out user from online DB
var myDB_deleteOnline = function(deleteUserID, callback) {
	var deleteUserIDSet =  {SS: [deleteUserID]};
	var params = {
		TableName: "online",
		Key: {
	      "users" : {S: "online"},
		},
	    UpdateExpression: "DELETE userIDs :d",
	    ExpressionAttributeValues : {
	      ":d": deleteUserIDSet
	    },
	}
	db.updateItem(params, function(err, data) {
	    if (err) {
	      console.log("Error", err);
	    }
		callback(err, data);
	});
}

// Adds a logged-in user to online DB
var myDB_addOnline = function(newUserID, callback) {
	var newUserIDSet = {SS: [newUserID]};
	var params = {
		TableName: "online",
		Key: {"users" : {S: "online"}},
	    UpdateExpression: "ADD userIDs :new",
	    ExpressionAttributeValues : {
	      ":new": newUserIDSet
	    },
	}
	db.updateItem(params, function(err, data) {
	    if (err) {
	      console.log("Error", err);
	    }
		callback(err, data);
	});
}

// Adds a chatID to user's invites list
var myDB_addInvite = function(username, chatID, callback) {
	var newChatIDSet = {SS: [chatID]};
	var params = {
		TableName: "users",
		Key: {"username" : {S: username}},
		UpdateExpression: "ADD invites :newChatID",
	    ExpressionAttributeValues : {
	      ":newChatID": newChatIDSet
	    },
	}
	db.updateItem(params, function(err, data) {
	    if (err) {
	      console.log("Error", err);
	    }
		callback(err, data);
	});
}

// Deletes a chatID from user's invite list
var myDB_deleteInvite = function(username, chatID, callback) {
  	var deleteChatIDSet ={SS: [chatID]};
  	var params = {
    	TableName: "users",
        Key: {"username" : {S: username}},
	    UpdateExpression: "DELETE invites :deleteChatID",
	    ExpressionAttributeValues : {
	      ":deleteChatID": deleteChatIDSet
	    },
    };
    db.updateItem(params, function(err, data) {
	    if (err) {
	      console.log("Error", err);
	    }
	    callback(err, data);
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

  addInvite: myDB_addInvite,
  deleteInvite: myDB_deleteInvite
};

module.exports = chatDB;