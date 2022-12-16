var AWS = require('aws-sdk');
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();

// Gets user's recommended articles titles
var myDB_getTitles = function(username, callback) {
  var params = {
      TableName: "users",
      Key: {"username" : {S: username}},
  };
  db.getItem(params, function(err, data) {
    if (err) {
      console.log("Error" + err);
    } else {
	  console.log(data.Item);
      callback(null, data.Item.recommended.SS);
    }
  });
}

// Gets user's recommended articles content
var myDB_getArticle = function(headline, callback) {
  var params = {
      TableName: "news",
      Key: {"headline" : {S: headline}},
  };
  db.getItem(params, function(err, data) {
    if (err) {
      console.log("Error" + err);
    } else {
	  console.log("this is the entire article: " + data.Item.L);
      callback(null, data.Item);
    }
  });
}


// Add like to news
var myDB_addLike = function(userID, likedArticle, callback) {
	var userSet = {SS: [userID]};
  	console.log(userID + " liked article" + likedArticle);

  	var paramsSet = {
		TableName: "news",
      	Key: {"headline" : {S: likedArticle}},
      	UpdateExpression: "ADD likes :a",
      	ExpressionAttributeValues : {
          ":a": userSet
        },
	};
    
	db.updateItem(paramsSet, function(err, data) {
	    if (err) {
	      	console.log("Error", err);
	    } else {
			callback(null, data.Item);
		} 
	});
}


// datePosted -> JS new Date() with the date given
// likesArr -> map undefined to empty string -> undefined(query empty condition)
// like: data.length











var newsDB = { 
  getTitles: myDB_getTitles,
  getArticle: myDB_getArticle,
  addLike: myDB_addLike,
};

module.exports = newsDB;