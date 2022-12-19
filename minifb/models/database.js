var AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-1' });
var db = new AWS.DynamoDB();

/* The function below is an example of a database method. Whenever you need to 
   access your database, you should define a function (myDB_addUser, myDB_getPassword, ...)
   and call that function from your routes - don't just call DynamoDB directly!
   This makes it much easier to make changes to your database schema. */

//gets username input and returns the password (this is just for one column)
var myDB_getPassword = function (searchTerm, callback) {
  var params = {
    KeyConditions: {
      username: {
        ComparisonOperator: 'EQ',
        AttributeValueList: [{ S: searchTerm }]
      }
    },
    TableName: "users",
    AttributesToGet: ['password']
  };

  db.query(params, function (err, data) {
    if (err || data.Items.length == 0) {
      callback(err, null);
    } else {
      callback(err, data.Items[0].password.S);
    }
  });
}

//gets username input and returns the username if existing
var myDB_getUsername = function (searchTerm, language, callback) {
  var params = {
    KeyConditions: {
      username: {
        ComparisonOperator: 'EQ',
        AttributeValueList: [{ S: searchTerm }]
      }
    },
    TableName: "users",
    AttributesToGet: ['username']
  };

  db.query(params, function (err, data) {
    if (err || data.Items.length == 0) {
      callback(err, null);
    } else {
      callback(err, data.Items[0].username.S);
    }
  });
}

//gets username input and gives the entire user entity
var myDB_userInfo = function (searchTerm, language, callback) {
  var params = {
    Key: {
      "username": {
        S: searchTerm
      }
    },
    TableName: "users"
  };
  db.getItem(params, function (err, data) {
    if (err) {
      console.log(err);
      callback(err, null);
    } else {
      callback(err, data.Item);
    }
  });
}

//create a new account with the right db parameters
var myDB_createAccount =
  function (newUsername, newPassword, newFullname, newAffiliation,
    newEmail, newBirthday, newInterest, newPfpURL, callback) {

    var interestArr = [];
    for (let i = 0; i < newInterest.length; i++) {
      var newIt =
      {
        "S": newInterest[0][i]
      }
      interestArr.push(newIt);
    }

    console.log(newUsername);
    console.log(newPassword);
    console.log(newFullname);
    console.log(newAffiliation);
    console.log(newEmail);
    console.log(newBirthday);
    console.log(newInterest);
    console.log(newPfpURL);

    var params = {
      TableName: "users",
      Item: {
        "username": { S: newUsername },
        "affiliation": { S: newAffiliation },
        "birthday": { S: newBirthday },
        "email": { S: newEmail },
        "fullname": { S: newFullname },
        "interest": { L: interestArr },
        "password": { S: newPassword },
        "pfpURL": { S: newPfpURL }
      }
    };

    db.putItem(params, function (err, data) {
      if (err) {
        console.log("error: " + err);
      }
    });
  }

//outputs friends
var myDB_getFriends = (function (username, callback) {
  var params = {
    TableName: "users",
    KeyConditions: {
      username: {
        ComparisonOperator: 'EQ',
        AttributeValueList: [{ S: username }]
      }
    },
    AttributesToGet: ['friends']
  };

  db.query(params, function (err, data) {
    if (err) {
      console.log(err);
    } else {
      if(data.Items[0].friends == undefined) {
        var empty = [];
        callback(err, empty);
      } else {
        callback(err, data.Items[0].friends.SS);
      }
    }
  });
});

//outputs all posts from user into an array
var myDB_allPosts = (function (userID, callback) {
  var params = {
    TableName: "posts",
    KeyConditionExpression: "userID = :a",
    ExpressionAttributeValues: {
      ":a": { S: userID }
    }
  };

  db.query(params, function (err, data) {
    if (err) {
      console.log(err);
    } else { //not sure if data.Items is all the items that has the key of userID???
      // data.Items.sort((a, b) => (a.timepost.S).localeCompare(b.timepost.S)).reverse();
      callback(err, data.Items);
    }
  });
});

// Update user email
var myDB_updateEmail = function (username, newEmail, callback) {
  var params = {
    TableName: "users",
    Item: {
      'username': { S: username },
      'email': { S: newEmail },
    }
  };

  db.putItem(params, function (err, data) {
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
var myDB_updatepw = function (username, newPw, callback) {
  var params = {
    TableName: "users",
    Item: {
      'username': { S: username },
      'password': { S: newPw },
    }
  };

  db.putItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else if (username.length == 0 || newPw.length == 0) {
      callback("Field cannot be left blank", null);
    } else {
      callback(err, "Updated");
    }
  });
}

//creates post with the right db parameters
var myDB_createPost = function (userID, content, timepost, callback) {
  var params = {
    TableName: "posts",
    Item: {
      "userID": {
        S: userID
      },
      "content": {
        S: content
      },
      "timepost": {
        S: timepost
      },
      "postType": {
        S: "posts"
      },
      "comments": {
        L: []
      }
    }
  };

  db.putItem(params, function (err, data) {
    if (err) {
      console.log(err);
    }
  });
}

//adds comment in post using userID (partition key) and timepost (sort key)
var myDB_addComment = function (userID, timepost, comment, table, callback) {
  var paramsGet;

  if(table === "posts") {
    paramsGet = {
      TableName: "posts",
      KeyConditionExpression: 'userID = :a and timepost = :b',
      ExpressionAttributeValues: {
        ':a': { S: userID },
        ':b': { S: timepost }
      }
    };
  } else {
    var userIDArray = [];
    userIDArray = userID.split(" ");
    var receiver = userIDArray[2];
    paramsGet = {
      TableName: "walls",
      KeyConditionExpression: 'receiver = :a and timepost = :b',
      ExpressionAttributeValues: {
        ':a': { S: receiver },
        ':b': { S: timepost }
      }
    };
  }

  db.query(paramsGet, function (err, data) {
    var tempArr = [];
    if(data != null) {
      tempArr = data.Items[0].comments.L;
    }
    var stringifyComment = {
      S: comment
    }

    tempArr.push(stringifyComment);

    var paramsUpdate;
    if(table === "posts") {
      paramsUpdate = {
        TableName: "posts",
        Key: {
          'userID': {
            S: userID
          },
          'timepost': {
            S: timepost
          },
        },
        UpdateExpression: 'SET comments = :c',
        ExpressionAttributeValues: {
          ':c': { L: tempArr }
        }
      };
    } else {
      paramsUpdate = {
        TableName: "walls",
        Key: {
          'receiver': {
            S: receiver
          },
          'timepost': {
            S: timepost
          },
        },
        UpdateExpression: 'SET comments = :c',
        ExpressionAttributeValues: {
          ':c': { L: tempArr }
        }
      };
    }

    db.updateItem(paramsUpdate, function (err, data) {
      if (err) {
        console.log(err);
      }
    });
  });
}

//outputs all walls from user into an array
var myDB_allWalls = (function (receiver, callback) {
  var params = {
    TableName: "walls",
    KeyConditionExpression: "receiver = :a",
    ExpressionAttributeValues: {
      ":a": { S: receiver }
    }
  };

  db.query(params, function (err, data) {
    if (err) {
      console.log(err);
    } else {
      callback(err, data.Items);
    }
  });
});

///query as sender
//outputs all walls as sender from user into an array
var myDB_allWallsAsSender = (function (receiver, sender, callback) {
  var params = {
    TableName: "walls",
    KeyConditionExpression: "receiver = :a",
    FilterExpression: 'contains (sender, :b)',
    ExpressionAttributeValues: {
      ":a": { S: receiver },
      ":b": { S: sender }
    }
  };

  db.query(params, function (err, data) {
    if (err) {
      console.log(err);
    } else {
      callback(err, data.Items);
    }
  });
});

//creates wall with the right db parameters
var myDB_createWall = function (receiver, sender, content, timepost, callback) {
  var params = {
    TableName: "walls",
    Item: {
      "receiver": {
        S: receiver
      },
      "sender": {
        S: sender
      },
      "content": {
        S: content
      },
      "timepost": {
        S: timepost
      },
      "postType": {
        S: "walls"
      },
      "comments": {
        L: []
      }
    }
  };

  db.putItem(params, function (err, data) {
    if (err) {
      console.log(err);
    }
  });
}

//update the userinfo
var myDB_updateUser = function (username, variable, columnName, callback) {
  var params = {
    Key: {
      "username": { S: username }
    },
    UpdateExpression: 'SET ' + columnName + ' = :c',
    ExpressionAttributeValues: {
      ':c': { S: variable }
    },
    TableName: "users",
  };

  db.updateItem(params, function (err, data) {
    if (err) {
      console.log("error: " + err);
    } else {
      callback("updated");
    }
  });
}

//updates interest to users database
var myDB_updateInterest = function (username, newInterest1, newInterest2, newInterest3, callback) {
  var interestArr = [];
  interestArr = [newInterest1, newInterest2, newInterest3];

  var paramsUpdate = {
    Key: {
      "username": { S: username }
    },
    UpdateExpression: 'SET interest = :c',
    ExpressionAttributeValues: {
      ':c': { L: interestArr }
    },
    TableName: "users",
  };


  db.updateItem(paramsUpdate, function (err, data) {
    if (err) {
      console.log("error: " + err);
    } else {
      var paramsGet = {
        KeyConditions: {
          username: {
            ComparisonOperator: 'EQ',
            AttributeValueList: [{ S: username }]
          }
        },
        TableName: 'users',
        AttributesToGet: ['interest']
      };

      db.query(paramsGet, function (err, data) {
        callback(err, data.Items[0].interest.L);
      });
    }
  });
}

//gets all interest from user
var myDB_getInterest = function (username, callback) {
  var paramsGet = {
    KeyConditions: {
      username: {
        ComparisonOperator: 'EQ',
        AttributeValueList: [{ S: username }]
      }
    },
    TableName: 'users',
    AttributesToGet: ['interest']
  };

  db.query(paramsGet, function (err, data) {
    callback(err, data.Items[0].interest.L);
  });
}

//get all the available user ids
var myDB_getAllUsername = (function (callback) {
  var params = {
    TableName: "users",
    ProjectionExpression: 'username'
  };

  db.scan(params, function (err, data) {
    if (err) {
      console.log(err);
    } else {
      callback(err, data.Items);
    }
  });
});

// Adds a friend request to user's DB
var myDB_addRequest = function(receiver, sender, callback) {

	var newUserIDSet = {SS: [sender]};
	var params = {
		TableName: "users",
		Key: {"username" : {S: receiver}},
		UpdateExpression: "ADD requests :newUserID",
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

// Deletes a friend request from user's db
var myDB_deleteRequest = function(receiver, sender, callback) {
  	var deleteUserIDSet = {SS: [sender]};
  	var params = {
    	TableName: "users",
        Key: {"username" : {S: receiver}},
	    UpdateExpression: "DELETE requests :deleteUserID",
	    ExpressionAttributeValues : {
	      ":deleteUserID": deleteUserIDSet
	    },
    };
    db.updateItem(params, function(err, data) {
	    if (err) {
	      console.log("Error", err);
	    }
	    callback(err, data);
    });
}

// Add user1 to user2's friends set
var myDB_addFriend = function(user1, user2, callback) {
	var add1To2 = {SS: [user1]};
	var params = {
		TableName: "users",
		Key: {"username" : {S: user2}},
		UpdateExpression: "ADD friends :newUserID",
	    ExpressionAttributeValues : {
	      ":newUserID": add1To2
	    },
	}
	db.updateItem(params, function(err, data) {
	    if (err) {
	      console.log("Error", err);
	    }
		callback(err, data);
	});
}

//add likes to the db with a string set of usernames (the size of the string set becomes the number of likes)
var myDB_addLike = function(userID, likedUser, timepost, postType, callback) {
	var userStringSet = {SS: [likedUser]};
  console.log(userID);
  console.log(likedUser);
  console.log(timepost);
  console.log(postType);


  var params;
  if(postType === "posts") {
    params = {
      TableName: "posts",
      Key: {
        'userID': {
          S: userID
        },
        'timepost': {
          S: timepost
        },
      },
      UpdateExpression: "ADD likes :a",
        ExpressionAttributeValues : {
          ":a": userStringSet
        },
    }
  } else {
    var userIDArray = [];
    userIDArray = userID.split(" ");
    var receiver = userIDArray[2];
    params = {
      TableName: "walls",
      Key: {
        'receiver': {
          S: receiver
        },
        'timepost': {
          S: timepost
        },
      },
      UpdateExpression: "ADD likes :a",
        ExpressionAttributeValues : {
          ":a": userStringSet
        },
    }
  }
    
	db.updateItem(params, function(err, data) {
	    if (err) {
	      console.log("Error", err);
	    }
      var paramsGet;

      if(postType === "posts") {
        paramsGet = {
          TableName: "posts",
          KeyConditionExpression: 'userID = :a and timepost = :b',
          ExpressionAttributeValues: {
            ':a': { S: userID },
            ':b': { S: timepost }
          }
        };
      } else {
        paramsGet = {
          TableName: "walls",
          KeyConditionExpression: "receiver = :a and timepost = :b",
          ExpressionAttributeValues: {
            ":a": { S: receiver },
            ":b": { S: timepost }
          }
        };
      }
        
      db.query(paramsGet, function (err, data) {
        if (err) {
          console.log(err);
        } else {
          if(data.Items[0].likes == undefined) {
            var empty = [];
            callback(err, empty);
          } else {
            callback(err, data.Items[0].likes.SS);
          }
        }
      });
	});
}

// Deletes a friend
var myDB_deleteFriend = function(username, friend, callback) {
  var friendSet = {SS: [friend]};
  var params = {
    TableName: "users",
    Key: {"username" : {S: username}},
    UpdateExpression: "DELETE friends :a",
    ExpressionAttributeValues : {
      ":a": friendSet
    },
  };
  db.updateItem(params, function(err, data) {
    if (err) {
      console.log("Error", err);
    }
    callback(err, data);
  });
}

// Updates user's affiliation
var myDB_addToAff = function(userID, updatedAffiliation, callback) {
	var newUserIDSet = {SS: [userID]};
	console.log("updated affil: " + updatedAffiliation);
	var params = {
		TableName: "affiliations",
		Key: {"affiliations" : {S: updatedAffiliation}},
	    UpdateExpression: "ADD users :new",
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

// Outputs user's affiliation
var myDB_getUserAffil = function (username, callback) {
  var params = {
    TableName: "users",
    Key: {"username" : {S: username}},
    AttributesToGet: ['affiliation'],
  };

  db.getItem(params, function(err, data) {
    if (err) {
      console.log("Error" + err);
    } else {
      callback(null, data.Item.affiliation.S);
    }
  });
}

// Gets a list of all the user's affiliation friends
var myDB_getAffiliations = function(affiliations, callback) {
  var params = {
      TableName: "affiliations",
      Key: {"affiliations" : {S: affiliations}},
  };

  db.getItem(params, function(err, data) {
    if (err) {
      console.log("Error" + err);
    } else {
      callback(null, data.Item.users.SS);
    }
  });
}


// TODO Your own functions for accessing the DynamoDB tables should go here

/* We define an object with one field for each method. For instance, below we have
   a 'lookup' field, which is set to the myDB_lookup function. In routes.js, we can
   then invoke db.lookup(...), and that call will be routed to myDB_lookup(...). */

// TODO Don't forget to add any new functions to this class, so app.js can call them. (The name before the colon is the name you'd use for the function in app.js; the name after the colon is the name the method has here, in this file.)

var database = {
  passwordLookup: myDB_getPassword,
  usernameLookup: myDB_getUsername,
  createAccount: myDB_createAccount,
  addFriend : myDB_addFriend,
  deleteRequest : myDB_deleteRequest,
  addRequest : myDB_addRequest,
  addLike : myDB_addLike,

  //NEW
  getAllPosts: myDB_allPosts,
  getFriends: myDB_getFriends,
  createPost: myDB_createPost,
  addComment: myDB_addComment,
  getAllWalls: myDB_allWalls,
  getAllWallsAsSender: myDB_allWallsAsSender,
  createWall: myDB_createWall,
  getUserInfo: myDB_userInfo,
  getInterest: myDB_getInterest,
  getAllUsername: myDB_getAllUsername,

  updateEmail: myDB_updateEmail,
  updatePw: myDB_updatepw,
  updateInterest: myDB_updateInterest,
  updateUser: myDB_updateUser,
  deleteFriend: myDB_deleteFriend,

  updateAffiliation: myDB_addToAff,
  getAffiliations: myDB_getAffiliations,
  getUserAffiliation: myDB_getUserAffil,

};

module.exports = database;