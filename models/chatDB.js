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
      callback(err, null);
    } else {
      callback(null, data.Item.chatID.L);
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
		  //data.Item.content.S, data.Item.chatroomName.S
	      callback(null, data.Item);
	    }
	});
}

// Adds a new chatroom with given info
var myDB_postChatroom = function(userID, createTime, userIDs, callback) {
	//otherUserIDs: set of strings
	var chatID = userID.concat(" ", createTime.toString());
	var params = {
		TableName: "chatrooms",
		Item: {
		  'chatID' : {S: chatID},
		  'userIDs' : {SS: userIDs},
		},
		//ConditionExpression: "attribute_not_exists(username)",
	}
	db.putItem(params, function(err, data) {
	//console.log(JSON.stringify(data));
	    if (err) {
	      callback(err, null);
	    } else {
	      callback(null, null);
	    }
	});
}

// Adds a new message to the chatroom
//content: [timestamp, userID, content]
var myDB_updateMessage = function(chatID, newMessage, callback) {
	var params = {
		TableName: "chatrooms",
		Key: {
	      "chatID" : {S: chatID},
		},
		//right syntax?
	    UpdateExpression: "SET #c = list_append(#c, :new)",
	    ExpressionAttributeNames: {
	      "#c": "content"
	    },
	    ExpressionAttributeValues : {
	      ":new": newMessage
	    },
	}
	db.updateItem(params, function(err, data) {
	//console.log(JSON.stringify(data));
	    if (err) {
	      callback(err, null);
	    } else {
	      callback(null, null);
	    }
	});
}

// When a user accepts a group chat invite, add the user to the groupchat and create the chatroom on the user's chat list
var myDB_addUserToChat = function(newUserID, groupChatID, callback) {
	var params = {
		TableName: "chatrooms",
		Key: {
	      "chatID" : {S: groupChatID},
		},
		
		UpdateExpression: "ADD userIDs :newUserID",
	    ExpressionAttributeValues : {
	      ":newUserID": newUserID
	    },
	}
	db.updateItem(params, function(err, data) {
	//console.log(JSON.stringify(data));
	    if (err) {
	      callback(err, null);
	    } else {
	      callback(null, null);
	    }
	});
}

// Adds a logged-in user to online DB
var myDB_addOnline = function(newUserID, callback) {
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
	      ":new": newUserID
	    },
	}
	db.updateItem(params, function(err, data) {
	//console.log(JSON.stringify(data));
	    if (err) {
	      callback(err, null);
	    } else {
	      callback(null, null);
	    }
	});
}

// Deletes a logged-out user from online DB
var myDB_deleteOnline = function(deleteUserID, callback) {
	var params = {
		TableName: "online",
		Key: {
	      "users" : {S: "online"},
		},
	    UpdateExpression: "DELETE #c :new",
	    ExpressionAttributeNames: {
	      "#c": "userIDs"
	    },
	    ExpressionAttributeValues : {
	      ":d": deleteUserID
	    },
	}
	db.updateItem(params, function(err, data) {
	    if (err) {
	      callback(err, null);
	    } else {
	      callback(null, null);
	    }
	});
}

var chatDB = { 
  getUserChatroomIDs: myDB_getUserChatrooms,
  
  getOnlineUsers: myDB_getOnlineUsers,
  
  getChatroom : myDB_getChatroom,
  addChatroom : myDB_postChatroom,
  getMessages : myDB_getChatMessages,
  addMessage : myDB_updateMessage,
  addUserToChat : myDB_addUserToChat,
  addUserOnline : myDB_addOnline,
  deleteUserOnline : myDB_deleteOnline,
};

module.exports = chatDB;
