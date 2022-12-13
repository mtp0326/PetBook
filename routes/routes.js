var db = require('../models/database.js');
var sjcl = require('sjcl');
// var stemmer = require('stemmer');

var isVerified = false;
// TODO The code for your own routes should go here
var getMain = function (req, res) {
  req.session.currWall = null;
  isVerified = false;
  res.render('main.ejs');
}

//render homepage
//NEW: getHomepage, homepage.ejs
var getHomepage = function (req, res) {
  req.session.currWall = null;
  if (!req.session.username) {
    return res.redirect('/')
  }
  res.render('homepage.ejs', { "check": isVerified })
}

var getWall = function (req, res) {
  req.session.currWall = req.session.username;
  if (!req.session.username) {
    return res.redirect('/')
  }
  isVerified = false;
  res.render('wall.ejs', { "check": true , "isOther": false, "username": req.session.username });
}

//for results if the username and password are correct
var postResultsUser = function (req, res) {
  req.session.currWall = null;
  var usernameCheck = req.body.username;
  var passwordCheck = req.body.password;
  var hashPassword = sjcl.codec.hex.fromBits(sjcl.hash.sha256.hash(passwordCheck));
  db.passwordLookup(usernameCheck, function (err, data) {
    if (data == hashPassword && !err) {
      req.session.username = req.body.username;
      req.session.password = req.body.password;
      isVerified = true;
      res.render('checklogin.ejs', { "check": true });
    } else {
      isVerified = false;
      res.render('checklogin.ejs', { "check": false });
    }
  });
}

//gets signup page
var getSignup = function (req, res) {
  req.session.currWall = null;
  res.render('signup.ejs', { "check": isVerified });
}

//gets logout page
var getLogout = function (req, res) {
  req.session.currWall = null;
  req.session.username = null;
  req.session.destroy();
  res.render('logout.ejs', {});
}

var getChat = function (req, res) {
  req.session.currWall = null;
  if (!req.session.username) {
    return res.redirect('/')
  }
  res.render('chat.ejs', { "check": isVerified })
}

var getEdit = function (req, res) {
  req.session.currWall = null;
  if (!req.session.username) {
    return res.redirect('/')
  }
  res.render('edit.ejs', { "check": isVerified })
}

var getOtherWallPageAjax = function (req, res) {
  if (!req.session.username) {
    return res.redirect('/')
  }
  console.log(req.body.searchcontent);
  req.session.currWall = req.body.searchcontent;
  console.log(req.session.currWall);
  db.usernameLookup(req.session.currWall, "username", function (err, data) {
    if (data === req.session.currWall) {
      res.render('wall.ejs', { "check": true, "isOther": true, "username": req.session.currWall });
      db.getUserInfo(req.session.currWall, "username", function(err, data) {
        console.log("otherUserWallAjax");
        console.log(data);
        res.send(data);
      })
    }
  });
}

//check if new account can be created by receiving null (which means that username in db is empty)
//and create the new account and go to restaurants or fail and go back to signup.
var postNewAccount = function (req, res) {
  var usernameNewCheck = req.body.username;
  db.usernameLookup(usernameNewCheck, "username", function (err, data) {
    if (data == null || err) {
      var hashPassword = sjcl.codec.hex.fromBits(sjcl.hash.sha256.hash(req.body.password));
      req.session.username = req.body.username;
      req.session.password = hashPassword;
      req.session.fullname = req.body.firstname + " " + req.body.lastname;
      req.session.affiliation = req.body.affiliation;
      req.session.email = req.body.email;
      req.session.birthday = req.body.birthday;

      var interestList = (req.body.interest.toLowerCase()).split(", ");
      req.session.interest = interestList;
      req.session.pfpURL = req.body.pfpURL;
      db.createAccount(req.session.username, req.session.password, req.session.fullname, req.session.affiliation, req.session.email, req.session.birthday,
        req.session.interest, req.session.pfpURL, function (err, data) { });
      isVerified = true;
      res.render('createaccount.ejs', { "check": true });
    } else {
      isVerified = false;
      res.render('createaccount.ejs', { "check": false });
    }

  });
}

//ajax: query posts of friend's userid
//Also renders comments if exists
//NEW: getHomepagePostList, getAllPosts
var getHomepagePostListAjax = function (req, res) {

  db.getFriends(req.session.username, function (err, data) {
    var friendsList = [];
    data.forEach(function (r) {
      friendsList.push(r);
    });
    console.log(friendsList);

    var tempList = [];
    db.getAllPosts(req.session.username, function (err, data) {
      var contentArr = data.map(obj => obj.content.S);
      var commentsArr = data.map(obj => obj.comments.L);
      var likesArr = data.map(obj => obj.likes.L);
      var userIDArr = data.map(obj => obj.userID.S);
      var timepostArr = data.map(obj => obj.timepost.S);

      for (let i = 0; i < userIDArr.length; i++) {
        var pointer = {
          "content": contentArr[i],
          "comments": commentsArr[i],
          "likes": likesArr[i],
          "userID": userIDArr[i],
          "timepost": timepostArr[i]
        };
        tempList.push(pointer);
      }

      db.getAllWalls(req.session.username, function (err, data) {
        var contentArr = data.map(obj => obj.content.S);
        var commentsArr = data.map(obj => obj.comments.L);
        var likesArr = data.map(obj => obj.likes.L);
        var userIDArr = data.map(obj => obj.sender.S + " to " + obj.receiver.S);
        var timepostArr = data.map(obj => obj.timepost.S);

        for (let i = 0; i < userIDArr.length; i++) {
          var pointer = {
            "content": contentArr[i],
            "comments": commentsArr[i],
            "likes": likesArr[i],
            "userID": userIDArr[i],
            "timepost": timepostArr[i]
          };
          tempList.push(pointer);
        }
        recGetAllPosts(friendsList, tempList, 0, function (postsList) {
          postsList.sort((a, b) => (a.timepost).localeCompare(b.timepost)).reverse();
          res.send(JSON.stringify(postsList));
        });
      });
    });
  });
}

var recGetAllPosts = function (recFriendsList, recPostsList, counter, callback) {
  if (counter >= recFriendsList.length) {
    callback(recPostsList);
  } else {
    db.getAllPosts(recFriendsList[counter], function (err, data) {
      var contentArr = data.map(obj => obj.content.S);
      var commentsArr = data.map(obj => obj.comments.L);
      var likesArr = data.map(obj => obj.likes.L);
      var userIDArr = data.map(obj => obj.userID.S);
      var timepostArr = data.map(obj => obj.timepost.S);

      for (let i = 0; i < userIDArr.length; i++) {
        var pointer = {
          "content": contentArr[i],
          "comments": commentsArr[i],
          "likes": likesArr[i],
          "userID": userIDArr[i],
          "timepost": timepostArr[i]
        };
        recPostsList.push(pointer);
      }

      db.getAllWalls(recFriendsList[counter], function (err, data) {
        var contentArr = data.map(obj => obj.content.S);
        var commentsArr = data.map(obj => obj.comments.L);
        var likesArr = data.map(obj => obj.likes.L);
        var userIDArr = data.map(obj => obj.sender.S + " to " + obj.receiver.S);
        var timepostArr = data.map(obj => obj.timepost.S);

        for (let i = 0; i < userIDArr.length; i++) {
          var pointer = {
            "content": contentArr[i],
            "comments": commentsArr[i],
            "likes": likesArr[i],
            "userID": userIDArr[i],
            "timepost": timepostArr[i]
          };
          recPostsList.push(pointer);
        }
        counter++;
        recGetAllPosts(recFriendsList, recPostsList, counter, callback);
      });
    });
  }
}

//ajax: get the creator information
var getCreator = function (req, res) {
  res.send(JSON.stringify(req.session.username));
}

//create new post in the db when all inputs exist in posts
var postNewPostAjax = function (req, res) {
  var content = req.body.content;
  var timepost = req.body.timepost;
  if (content.length != 0 && timepost.length != 0) {
    db.createPost(req.session.username, content, timepost, function (err, data) {});

    var response = {
      "userID": req.session.username,
      "content": content,
      "timepost": timepost
    };

    res.send(response);
  } else {
    res.send(null);
  }
}

//ajax: add comment in post data in posts
var postNewCommentAjax = function (req, res) {
  var userID = req.body.userID;
  var timepost = req.body.timepost;
  var comment = req.body.comment;
  console.log("userID " + userID);
  console.log("timepost " + timepost);
  console.log("comment " + comment);

  if (userID.length != 0 && timepost.length != 0 && comment.length != 0) {
    console.log("passing");
    db.addComment(userID, timepost, comment, function (err, data) { });

    var response = {
      "userID": userID,
      "timepost": timepost,
      "comment": comment
    };

    res.send(response);
  } else {
    res.send(null);
  }
}

//ajax: get your posts and wall you receive from friends posting on yours
//NEW
var getWallListAjax = function (req, res) {
  console.log(req.session.username);
  var tempList = [];
  ///req.session.username into B's wall
  db.getAllPosts(req.session.currWall, function (err, data) {
    var contentArr = data.map(obj => obj.content.S);
    var commentsArr = data.map(obj => obj.comments.L);
    var likesArr = data.map(obj => obj.likes.L);
    var userIDArr = data.map(obj => obj.userID.S);
    var timepostArr = data.map(obj => obj.timepost.S);

    for (let i = 0; i < userIDArr.length; i++) {
      var pointer = {
        "content": contentArr[i],
        "comments": commentsArr[i],
        "likes": likesArr[i],
        "userID": userIDArr[i],
        "timepost": timepostArr[i]
      };
      tempList.push(pointer);
    }
    ///req.session.username into B's wall
    console.log("getCurrWall");
    console.log(req.session.currWall);
    db.getAllWalls(req.session.currWall, function (err, postsList) {
      var contentArr = postsList.map(obj => obj.content.S);
      var commentsArr = postsList.map(obj => obj.comments.L);
      var likesArr = postsList.map(obj => obj.likes.L);
      var userIDArr = postsList.map(obj => obj.sender.S + " to " + obj.receiver.S);
      var timepostArr = postsList.map(obj => obj.timepost.S);

      for (let i = 0; i < userIDArr.length; i++) {
        var pointer = {
          "content": contentArr[i],
          "comments": commentsArr[i],
          "likes": likesArr[i],
          "userID": userIDArr[i],
          "timepost": timepostArr[i]
        };
        tempList.push(pointer);
      }

      db.getFriends(req.session.currWall, function (err, data) {
        var friendsList = [];
        data.forEach(function (r) {
          friendsList.push(r.S);
        });

        ///recursion as friends
        recGetAllWalls(friendsList, tempList, req.session.username, 0, function (postsList) {
          console.log("postsList");
          console.log(postsList);
          if (postsList.length > 1) {
            postsList.sort((a, b) => (a.timepost).localeCompare(b.timepost)).reverse();
          }
          console.log(postsList);
          res.send(JSON.stringify(postsList));

          if (err) {
            console.log("error" + err);
          }
        });
      });
    });
  });
}

var recGetAllWalls = function (recFriendsList, recWallsList, sender, counter, callback) {
  if (counter >= recFriendsList.length) {
    console.log("recWallsList");
    console.log(recWallsList);
    callback(recWallsList);
  } else {
    db.getAllWallsAsSender(recFriendsList[counter], sender, function (err, data) {
      console.log("asSe" + data);
      var contentArr = data.map(obj => obj.content.S);
      var commentsArr = data.map(obj => obj.comments.L);
      var likesArr = data.map(obj => obj.likes.L);
      var userIDArr = data.map(obj => obj.sender.S + " to " + obj.receiver.S);
      var timepostArr = data.map(obj => obj.timepost.S);

      for (let i = 0; i < userIDArr.length; i++) {
        var pointer = {
          "content": contentArr[i],
          "comments": commentsArr[i],
          "likes": likesArr[i],
          "userID": userIDArr[i],
          "timepost": timepostArr[i]
        };
        recWallsList.push(pointer);
      }
      counter++;
      recGetAllWalls(recFriendsList, recWallsList, sender, counter, callback);
    });
  }
}

//create new wall in the db when all inputs exist in posts
var postNewWallAjax = function (req, res) {
  var content = req.body.content;
  var timepost = req.body.timepost;
  if (content.length != 0 && timepost.length != 0 && receiver.length != 0) {
    db.createWall(req.session.currWall, req.session.username, content, timepost, function (err, data) { });

    var response = {
      "userID": req.session.username + " to " + receiver,
      "content": content,
      "timepost": timepost
    };

    res.send(response);
  } else {
    res.send(null);
  }
}

var getEditUserInfoAjax = function (req, res) {
  console.log("getUser");
  db.getUserInfo(req.session.username, "username", function (err, data) {
    console.log(data);
    res.send(data);
  });
}

var postUpdateUser = function (req, res) {
  var updateInfoList = [];
  var hashPassword = sjcl.codec.hex.fromBits(sjcl.hash.sha256.hash(req.body.password));
  updateInfoList.push(req.body.affiliation);
  updateInfoList.push(req.body.email);
  updateInfoList.push(req.body.firstname + " " + req.body.lastname);
  updateInfoList.push(hashPassword);
  updateInfoList.push(req.body.pfpURL);

  var updateInfoNameList = [];
  updateInfoNameList.push('affiliation');
  updateInfoNameList.push('email');
  updateInfoNameList.push('fullname');
  updateInfoNameList.push('password');
  updateInfoNameList.push('pfpURL');

  console.log(updateInfoList);
  console.log(updateInfoNameList);

  res.render('editaccount.ejs', { "check": true });

  db.getInterest(req.session.username, function (err,data) {
    var interestSet = new Set();
    for(let i = 0; i < data.length; i++) {
      interestSet.add(data[i].S);
    }

    recUpdateUser(req.session.username, updateInfoList, updateInfoNameList, 0, function(err, message) {
      if(err) {
        console.log(err);
      }
      db.updateInterest(req.session.username, req.body.interest, function(err, data) {
        if(err) {
          console.log(err);
        }
        console.log(interestSet);
        for(let i = 0; i < data.length; i++) {
          console.log(interestSet);
          console.log(data[i].S);
          if(!interestSet.has(data[i].S)) {
            var newContent = req.session.username + " is now interested in " + data[i].S;
            var newTimepost = new Date().getTime() + "";
            db.createPost(req.session.username, newContent, newTimepost, function(err, data){});
          }
        }
      });
    });
  });
}

var recUpdateUser = function (sessionUser, recUpdateInfoList, recUpdateInfoNameList, counter, callback) {
  if (counter >= recUpdateInfoList.length) {
    callback("successfully updated user");
  } else {
    db.updateUser(sessionUser, recUpdateInfoList[counter], recUpdateInfoNameList[counter], function (err, data) {
      console.log("callback hello");
      counter++;
      recUpdateUser(sessionUser, recUpdateInfoList, recUpdateInfoNameList, counter, callback);
    });
  }
}


//***************************************************** */

//get all restaurants and login verification and put in restaurants
var getRestaurants = function (req, res) {
  res.render('restaurants.ejs', { "isVerified": isVerified })
};



//ajax: get all data of restaurants
var getRestaurantList = function (req, res) {
  db.getAllRestaurants(function (err, data) {
    res.send(JSON.stringify(data))
  });
};



//create new restaurant in the db when all inputs exist
var postNewRestaurantAjax = function (req, res) {
  var latitude = req.body.latitude;
  var longitude = req.body.longitude;
  var resName = req.body.name;
  var description = req.body.description;
  if (latitude.length != 0 && longitude.length != 0 && resName.length != 0 && description.length != 0) {
    db.createRestaurant(resName, latitude, longitude, description, req.session.username, function (err, data) { });

    var response = {
      "name": resName,
      "latitude": latitude,
      "longitude": longitude,
      "description": description,
      "creator": req.session.username
    };

    res.send(response);
  } else {
    res.send(null);
  }
};

//ajax: deletes the restaurant data in db
var postDeleteRestaurantAjax = function (req, res) {
  var resName = req.body.name;
  db.deleteRestaurant(resName, function (err, data) { });
  res.send(resName);
};

//create new restaurant in the db when all inputs exist
//var postNewRestaurant = function(req, res) {
//  var latitude = req.body.latitude;
//  var longitude = req.body.longitude;
//  var resName = req.body.restaurantName;
//  var description = req.body.description;
//  if(latitude.length != 0 && longitude.length != 0 && resName.length != 0 && description.length != 0) {
//	db.createRestaurant(resName, latitude, longitude, description, session.username, function(err, data){});
//	
//  	res.render('addrestaurant.ejs', {"allFields" : true}); //send? addrestaurant.ejs is unnecessary
//  } else {
//	res.render('addrestaurant.ejs', {"allFields" : false});
//  }
//};

// TODO Don't forget to add any new functions to this class, so app.js can call them. (The name before the colon is the name you'd use for the function in app.js; the name after the colon is the name the method has here, in this file.)

var routes = {
  get_main: getMain,
  verifyUser: postResultsUser,
  get_restaurants: getRestaurants,
  get_restaurantList: getRestaurantList,
  get_signup: getSignup,
  get_logout: getLogout,
  get_creator: getCreator,
  get_chat: getChat,
  get_wall: getWall,
  get_otherWallPageAjax: getOtherWallPageAjax,
  get_edit: getEdit,

  //NEW
  get_homepage: getHomepage,
  get_homepagePostListAjax: getHomepagePostListAjax,
  get_wallListAjax: getWallListAjax,
  get_editUserInfoAjax: getEditUserInfoAjax,

  post_newPostAjax: postNewPostAjax,
  post_newCommentAjax: postNewCommentAjax,
  post_newWallAjax: postNewWallAjax,
  post_updateUser: postUpdateUser,

  post_newAccount: postNewAccount,
  post_newRestaurantAjax: postNewRestaurantAjax,
  post_deleteRestaurantAjax: postDeleteRestaurantAjax

  //post_newRestaurant : postNewRestaurant
};

module.exports = routes;