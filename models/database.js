var AWS = require('aws-sdk');
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();
//new db??

/* The function below is an example of a database method. Whenever you need to 
   access your database, you should define a function (myDB_addUser, myDB_getPassword, ...)
   and call that function from your routes - don't just call DynamoDB directly!
   This makes it much easier to make changes to your database schema. */

//gets username input and returns the password
var myDB_getPassword = function(searchTerm, language, callback) {
  var params = {
      KeyConditions: {
        username: {
          ComparisonOperator: 'EQ',
          AttributeValueList: [ { S: searchTerm } ]
        }
      },
      TableName: "users",
      AttributesToGet: [ 'password' ]
  };

  db.query(params, function(err, data) {
    if (err || data.Items.length == 0) {
      callback(err, null);
    } else {
      callback(err, data.Items[0].password.S);
    }
  });
}

//gets username input and returns the username if existing
var myDB_getUsername = function(searchTerm, language, callback) {
  var params = {
      KeyConditions: {
        username: {
          ComparisonOperator: 'EQ',
          AttributeValueList: [ { S: searchTerm } ]
        }
      },
      TableName: "users",
      AttributesToGet: [ 'username' ]
  };

  db.query(params, function(err, data) {
    if (err || data.Items.length == 0) {
      callback(err, null);
    } else {
      callback(err, data.Items[0].username.S);
    }
  });
}

//NEW
//create a new account with the right db parameters
var myDB_createAccount =
  function(newUsername, newPassword, newFullname, newAffiliation,
    newBirthday, newEmail, newChatID, newFriends, newInterest, newPfpURL, callback) {
  	var params = {
      TableName: "users",
      Item : {
        "username": { S: newUsername },
        "password": { S: newPassword },
        "fullname": { S: newFullname },
        "affiliation": { S: newAffiliation },
        "birthday": { S: newBirthday },
        "email": { S: newEmail },
        "chatID": { L: newChatID },
        "friends": { SS: newFriends },
        "interest": { L: newInterest },
        "pfpURL": { S: newPfpURL }
      }
    };

  db.putItem(params, function(err, data) {
    if (err) {
		console.log(err)
    }
  });
}

//NEW
//outputs friends
var myDB_getFriends = (username, function(callback) {
  var params = {
  TableName: "users",
    Key: {"username" : {S: username}},
    ExpressionAttributeValues: "friends"
  };

  db.query(params, function(err, data) {
    if(err) {
      console.log(err);
    } else {
      callback(err, data.Items);
    }
  });
});

//NEW
//outputs all restaurants from db into an array
var myDB_allPosts = (userID, function(callback) {
  var params = {
  TableName: "posts",
    Key: {"userID" : {S: userID}}
  };

  db.query(params, function(err, data) {
    if(err) {
      console.log(err);
    } else { //not sure if data.Items is all the items that has the key of userID???
      data.Items.sort((a, b) => (a.timestamp.S).localeCompare(b.timestamp.S)).reverse();
      callback(err, data.Items);
    }
  });
});





//creates restaurant with the right db parameters
var myDB_createRestaurant = function(name, latitude, longitude, description, creator, callback) {
  	var params = {
    TableName: "restaurants",
      Item : {
        "name" : {
          S: name
        },
        "latitude": {
          S: latitude
        },
        "longitude": {
          S: longitude
        },
        "description": {
          S: description
        },
        "creator": {
          S: creator
        }
        }
      };

  db.putItem(params, function(err, data) {
    if (err) {
		console.log(err);
    }
  });
}




//deletes restaurant using key and tablename
var myDB_deleteRestaurant = function(name, callback) {
  var params = {
    TableName: "restaurants",
    Key : {
        "name" : {
          S: name
        }
    }
  };

  db.deleteItem(params, function(err, data) {
    if (err) {
    console.log(err);
    }
  });
}

//outputs all restaurants from db into an array
var myDB_allRestaurants = function(callback) {
  	var params = {
		TableName: "restaurants",
		Select: "ALL_ATTRIBUTES"
  	};

  	db.scan(params, function(err, data) {
	if(err) {
		console.log(err);
	} else {
		callback(err, data.Items);
	}
  });
}

//GERMAN
var myDB_lookup = function(searchTerm, language, callback) {
  console.log('Looking up: ' + searchTerm); 

  var params = {
      KeyConditions: {
        keyword: {
          ComparisonOperator: 'EQ',
          AttributeValueList: [ { S: searchTerm } ]
        }
      },
      TableName: "words",
      AttributesToGet: [ 'German' ]
  };

  db.query(params, function(err, data) {
    if (err || data.Items.length == 0) {
      callback(err, null);
    } else {
      callback(err, data.Items[0].German.S);
    }
  });
}

// TODO Your own functions for accessing the DynamoDB tables should go here

/* We define an object with one field for each method. For instance, below we have
   a 'lookup' field, which is set to the myDB_lookup function. In routes.js, we can
   then invoke db.lookup(...), and that call will be routed to myDB_lookup(...). */

// TODO Don't forget to add any new functions to this class, so app.js can call them. (The name before the colon is the name you'd use for the function in app.js; the name after the colon is the name the method has here, in this file.)

var database = { 
  lookup: myDB_lookup,
  passwordLookup: myDB_getPassword,
  usernameLookup: myDB_getUsername,
  createAccount: myDB_createAccount,

  //NEW
  getAllPosts : myDB_allPosts,
  getFriends : myDB_getFriends,
  
  createRestaurant : myDB_createRestaurant,
  getAllRestaurants : myDB_allRestaurants,
  deleteRestaurant : myDB_deleteRestaurant

  
};

module.exports = database;
                                        