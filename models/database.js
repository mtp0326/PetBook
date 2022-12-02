var AWS = require('aws-sdk');
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();

/* The function below is an example of a database method. Whenever you need to 
   access your database, you should define a function (myDB_addUser, myDB_getPassword, ...)
   and call that function from your routes - don't just call DynamoDB directly!
   This makes it much easier to make changes to your database schema. */

//gets username input and returns the password
var myDB_getPassword = function(searchTerm, callback) {
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
    console.log(data);
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
    newEmail, newBirthday, newInterest, newPfpURL, callback) {

      console.log(newUsername
      + " " + newPassword
      + " " + newFullname
      + " " + newAffiliation
      + " " + newEmail
      + " " + newBirthday
      + " " + newInterest
      + " " + newPfpURL);
      var interestArr =[];
      for(let i = 0; i < newInterest.length; i++){
        var newIt =
        {
          "S" : newInterest[i]
        }
        interestArr.push(newIt);
      }

      var params = {
        TableName: "users",
        Item : {
          "username": { S: newUsername },
          "affiliation": { S: newAffiliation },
          "birthday": { S: newBirthday },
          "chatID": { L: [] },
          "email": { S: newEmail },
          "friends": { L: [] },
          "fullname": { S: newFullname },
          "interest": { L: interestArr },
          "password": { S: newPassword },
          "pfpURL": { S: newPfpURL }
        }
      };
      console.log(params);

  db.putItem(params, function(err, data) {
    console.log(data);
    if (err) {
      console.log("error");
		  console.log(err)
    }
  });
}

//NEW
//outputs friends
var myDB_getFriends = (function(username, callback) {
  var params = {
  TableName: "users",
    KeyConditions: {
      username: {
        ComparisonOperator: 'EQ',
        AttributeValueList: [ { S: username } ]
      }
    },
    TableName: "users",
    AttributesToGet: [ 'friends' ]
  };

  db.query(params, function(err, data) {
    if(err) {
      console.log(err);
    } else {
      console.log(data.Items[0].friends.L);
      callback(err, data.Items[0].friends.L);
    }
  });
});

//NEW
//outputs all posts from user into an array
var myDB_allPosts = (function(userID, callback) {
  var params = {
    TableName: "posts",
    KeyConditionExpression: "userID = :a",
    ExpressionAttributeValues: {
      ":a": { S: userID }
    }
  };

  db.query(params, function(err, data) {
    if(err) {
      console.log(err);
    } else { //not sure if data.Items is all the items that has the key of userID???
      // data.Items.sort((a, b) => (a.timestamp.S).localeCompare(b.timestamp.S)).reverse();
      console.log(data.Items);
      callback(err, data.Items);
    }
  });
});

// Update user email
var myDB_updateEmail = function(username, newEmail, callback) {
	var params = {
		TableName: "users",
  		Item: {
			'username': {S: username},
			'email': {S: newEmail},
		}
	};
	
	db.putItem(params, function(err, data) {
	    if (err) {
	      callback(err, null);
	    } else if (username.length == 0 || newEmail.length == 0) {
		  callback("Field cannot be left blank", null);
		} else {
	      callback(err, "Updated");
	    }
    });
}

// Update user password
var myDB_updatepw = function(username, newPw, callback) {
	var params = {
		TableName: "users",
  		Item: {
			'username': {S: username},
			'password': {S: newPw},
		}
	};
	
	db.putItem(params, function(err, data) {
	    if (err) {
	      callback(err, null);
	    } else if (username.length == 0 || newPw.length == 0) {
		  callback("Field cannot be left blank", null);
		} else {
	      callback(err, "Updated");
	    }
    });
}

// Update user interest. Minimum 2???
//client side has the list of interests => newInterests is final interests
var myDB_updateInterest = function(username, newInterests, callback) {
	var params = {
		TableName: "users",
  		Item: {
			'username': {S: username},
			'interest': {S: newInterests},
		}
	};
	
	db.putItem(params, function(err, data) {
	    if (err) {
	      callback(err, null);
	    } else if (username.length == 0 || newPw.length == 0) {
		  callback("Field cannot be left blank", null);
		} else {
	      callback(err, "Updated");
	    }
    });
}




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

  updateEmail : myDB_updateEmail,
  updatePw : myDB_updatepw,
  updateInterest : myDB_updateInterest,
  
  createRestaurant : myDB_createRestaurant,
  getAllRestaurants : myDB_allRestaurants,
  deleteRestaurant : myDB_deleteRestaurant

  
};

module.exports = database;
                                        