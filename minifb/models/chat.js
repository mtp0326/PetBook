var AWS = require('aws-sdk');
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();
//var docClient = new AWS.DynamoDB.DocumentClient();

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





var chatDB = { 
  getChatroom : myDB_getChatroom,
  addChatroom : myDB_postChatroom,
  addMessage : myDB_updateMessage,
};

module.exports = chatDB;