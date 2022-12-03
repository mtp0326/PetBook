var AWS = require('aws-sdk');
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();
//var docClient = new AWS.DynamoDB.DocumentClient();

// Gets all of the user's chatrooms ids
var myDB_getUserChatrooms = function(username, callback) {
  var params = {
      TableName: "users",
      Key: {"username" : {S: username}},
  };

  db.getItem(params, function(err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Item);
    }
  });
}

// Gets the chatroom info with a given chatID
var myDB_getChatroom = function(chatID, callback) {
	var params = {
        TableName: "chatrooms",
        Key: {"chatID" : {S: chatID}},
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
var myDB_postChatroom = function(chatID, userIDs, chatroomName, currTime, callback) {
	//chatID: userID + timestamp
	//otherUserIDs: list of strings
	var params = {
		TableName: "chatrooms",
		Item: {
		  'chatID' : {S: chatID},
		  'userIDs' : {SS: userIDs},
		  'lastMessageTime' : {S: currTime},
		  'chatroomName' : {S: chatroomName},
		},
		ConditionExpression: "attribute_not_exists(username)",
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
var myDB_updateMessage = function(chatID, content, callback) {
	var params = {
		TableName: "chatrooms",
		Key: {"chatID" : {S: chatID}},
		
		//right syntax?
	    UpdateExpression: "SET #c = list_append(#c, :new)",
	    ExpressionAttributeNames: {
	      "#c": "content"
	    },
	    ExpressionAttributeValues : {
	      ":new": content
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

// Gets the chatroom's messages
/*
var myDB_getChatMessages = function(chatID, callback) {
  	var params = {
      	TableName: "chatrooms",
      	Key: {"chatID" : {S: chatID}},
  	};

	db.getItem(params, function(err, data) {
	  	if (err) {
	      callback(err, null);
	    } else {
	      callback(null, data.Item);
	    }
	});
}*/

// When a user accepts a group chat invite, add the user to the groupchat and create the chatroom on the user's chat list
var myDB_addUserToChat = function(newUserID, groupchatID, callback) {
	var params = {
		TableName: "chatrooms",
		Key: {"chatID" : {S: groupchatID}},
		
		//right syntax?
		UpdateExpression: "ADD userIDs :newUserID",
//	    UpdateExpression: "ADD #u :newUserID",
//	    ExpressionAttributeNames: {
//	      "#u": "userIDs"
//	    },
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

var chatDB = { 
  getUserChatroomIDs: myDB_getUserChatrooms,
  
  getChatroom : myDB_getChatroom,
  addChatroom : myDB_postChatroom,
  getMessages : myDB_getChatMessages,
  addMessage : myDB_updateMessage,
  addUserToChat : myDB_addUserToChat,
};

module.exports = chatDB;
