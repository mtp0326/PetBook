var db = require('../models/database.js');
var chatdb = require('../models/chatDB.js');
var sjcl = require('sjcl');
const { exec } = require('child_process');
// var stemmer = require('stemmer');

//get the starting page (login)
var getMain = function (req, res) {
  req.session.currWall = null;
  req.session.isVerified = false;
  res.render('main.ejs');
}

//render homepage
var getHomepage = function (req, res) {
  req.session.currWall = null;
  if (!req.session.username) {
    return res.redirect('/')
  }
  res.render('homepage.ejs', { "check": req.session.isVerified })
}

//render news
var getNews = function (req, res) {
  req.session.currWall = null;
  if (!req.session.username) {
    return res.redirect('/')
  }
  res.render('news.ejs', { "check": req.session.isVerified })
}

//render news search
var getNewsSearch = function (req, res) {
  req.session.currWall = null;
  if (!req.session.username) {
    return res.redirect('/')
  }
  res.render('newsSearch.ejs', { "check": req.session.isVerified })
}

//render news search result
var getNewsSearchResult = function (req, res) {
  req.session.currWall = null;
  if (!req.session.username) {
    return res.redirect('/')
  }
  res.render('newsSearchResult.ejs', { "check": req.session.isVerified })
}

//render wall page
var getWall = function (req, res) {
  req.session.currWall = req.session.username;
  if (!req.session.username) {
    return res.redirect('/')
  }
  res.render('wall.ejs', { "check": req.session.isVerified, "isOther": false, "username": req.session.username});
}

//render other user's wall page
var getOtherWall = function (req, res) {
  req.session.currWall = req.session.username;
  if (!req.session.username) {
    return res.redirect('/')
  }
  res.render('wall.ejs', { "check": req.session.isVerified, "isOther": true, "username": "other"});
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
      req.session.isVerified = true;
      res.render('checklogin.ejs', { "check": req.session.isVerified });
    } else {
      req.session.isVerified = false;
      res.render('checklogin.ejs', { "check": req.session.isVerified });
    }
  });
}

//gets signup page
var getSignup = function (req, res) {
  req.session.currWall = null;
  res.render('signup.ejs', { "check": req.session.isVerified });
}

//gets logout page
var getLogout = function (req, res) {
  req.session.currWall = null;
  req.session.isVerified = false;
  res.render('main.ejs', {});
	
		chatdb.deleteUserOnline(req.session.username, function (err, data) {
			if (err) { console.log(err); }
      req.session.username = null;
	  req.session.destroy();
		})
}

//render chat
var getChat = function (req, res) {
  req.session.currWall = null;
  if (!req.session.username) {
    return res.redirect('/')
  }
  res.render('chat.ejs', { "check": req.session.isVerified })
}

//render edit
var getEdit = function (req, res) {
  req.session.currWall = null;
  if (!req.session.username) {
    return res.redirect('/')
  }
  res.render('edit.ejs', { "check": req.session.isVerified })
}

//add the req.session.currWall the user that was searched
var postOtherWallPageAjax = function (req, res) {
  if (!req.session.username) {
    return res.redirect('/')
  }
  req.session.currWall = req.body.currWall;
  res.send("success");
}

//gives information about the wall owner
var getDetermineWallOwner = function (req, res) {
  db.usernameLookup(req.session.currWall, "username", function (err, data) {
   
    if (data === null) {
      req.session.currWall = null;
      res.send("null");
    } else {
      db.getUserInfo(req.session.currWall, "username", function (err, data) {
        res.send(data);
      })
    }

  });
}

//give information about user
var getUserInfo = function(req, res){
  db.getUserInfo(req.session.username,  "username", function (err, data) {
    res.send(data);
  })
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
      req.session.interest1 = req.body.interest1;
      req.session.interest2 = req.body.interest2;
      req.session.interest3 = req.body.interest3;
      
      // var stringify1 = { 
      //   S: req.session.interest1
      // };
      // var stringify2 = { 
      //   S: req.session.interest2
      // };
      // var stringify3 = { 
      //   S: req.session.interest3
      // };
      var interestList = [req.session.interest1,req.session.interest2,req.session.interest3];
      req.session.interest = interestList;
      req.session.pfpURL = req.body.pfpURL;
      db.createAccount(req.session.username, req.session.password, req.session.fullname, req.session.affiliation, req.session.email, req.session.birthday,
        interestList, req.session.pfpURL, function (err, data) { });
        req.session.isVerified = true;
      res.render('createaccount.ejs', { "check": req.session.isVerified });
    } else {
      req.session.isVerified = false;
      res.render('createaccount.ejs', { "check": req.session.isVerified });
    }

  });
}

//ajax: query posts of friend's userid
//Also renders comments if exists
//NEW: getHomepagePostList, getAllPosts
var getHomepagePostListAjax = function (req, res) {

  var tempList = [];
  db.getAllPosts(req.session.username, function (err, data) {
    var contentArr = data.map(obj => obj.content.S);
    var commentsArr = data.map(obj => obj.comments.L);
    var userIDArr = data.map(obj => obj.userID.S);
    var timepostArr = data.map(obj => obj.timepost.S);
    var postTypeArr = data.map(obj => obj.postType.S);
    var likesArr = data.map(function(obj) {
      if(obj.likes == undefined) {
        return "0";
      } else {
        return obj.likes.SS.length+"";
      }
    })

    for (let i = 0; i < userIDArr.length; i++) {
      var pointer = {
        "content": contentArr[i],
        "comments": commentsArr[i],
        "userID": userIDArr[i],
        "timepost": timepostArr[i],
        "postType": postTypeArr[i],
        "likes": likesArr[i]
      };
      tempList.push(pointer);
    }

    db.getAllWalls(req.session.username, function (err, data) {
      var contentArr = data.map(obj => obj.content.S);
      var commentsArr = data.map(obj => obj.comments.L);
      var userIDArr = data.map(obj => obj.sender.S + " to " + obj.receiver.S);
      var timepostArr = data.map(obj => obj.timepost.S);
      var postTypeArr = data.map(obj => obj.postType.S);
      var likesArr = data.map(function(obj) {
        if(obj.likes == undefined) {
          return "0";
        } else {
          return obj.likes.SS.length+"";
        }
      })

      for (let i = 0; i < userIDArr.length; i++) {
        var pointer = {
          "content": contentArr[i],
          "comments": commentsArr[i],
          "userID": userIDArr[i],
          "timepost": timepostArr[i],
          "postType": postTypeArr[i],
          "likes": likesArr[i]
        };
        tempList.push(pointer);
      }
      db.getFriends(req.session.username, function (err, data) {

        var friendsList = [];
        data.forEach(function (r) {
          friendsList.push(r);
        });
        if (friendsList[0] === "" && friendsList.length === 1) {
          res.send(JSON.stringify(tempList));
        } else {
          recGetAllPosts(friendsList, tempList, 0, function (postsList) {
            postsList.sort((a, b) => (a.timepost).localeCompare(b.timepost)).reverse();
            res.send(JSON.stringify(postsList));
          });
        }
      });
    });
  });
}

//uses recursion to get friends posts
var recGetAllPosts = function (recFriendsList, recPostsList, counter, callback) {
  if (counter >= recFriendsList.length) {
    callback(recPostsList);
  } else {
    db.getAllPosts(recFriendsList[counter], function (err, data) {
      var contentArr = data.map(obj => obj.content.S);
      var commentsArr = data.map(obj => obj.comments.L);
      var userIDArr = data.map(obj => obj.userID.S);
      var timepostArr = data.map(obj => obj.timepost.S);
      var postTypeArr = data.map(obj => obj.postType.S);
      var likesArr = data.map(function(obj) {
      if(obj.likes == undefined) {
        return "0";
      } else {
        return obj.likes.SS.length+"";
      }
    })

      for (let i = 0; i < userIDArr.length; i++) {
        var pointer = {
          "content": contentArr[i],
          "comments": commentsArr[i],
          "userID": userIDArr[i],
          "timepost": timepostArr[i],
          "postType": postTypeArr[i],
          "likes": likesArr[i]
        };
        recPostsList.push(pointer);
      }

      db.getAllWalls(recFriendsList[counter], function (err, data) {
        var contentArr = data.map(obj => obj.content.S);
        var commentsArr = data.map(obj => obj.comments.L);
        var userIDArr = data.map(obj => obj.sender.S + " to " + obj.receiver.S);
        var timepostArr = data.map(obj => obj.timepost.S);
        var postTypeArr = data.map(obj => obj.postType.S);
        var likesArr = data.map(function(obj) {
          if(obj.likes == undefined) {
            return "0";
          } else {
            return obj.likes.SS.length+"";
          }
        })

        for (let i = 0; i < userIDArr.length; i++) {
          var pointer = {
            "content": contentArr[i],
            "comments": commentsArr[i],
            "userID": userIDArr[i],
            "timepost": timepostArr[i],
            "postType": postTypeArr[i],
            "likes": likesArr[i]
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
  var postType = {
    S: "posts"
  };
  if (content.length != 0 && timepost.length != 0) {
    db.createPost(req.session.username, content, timepost, function (err, data) { });

    var response = {
      "userID": req.session.username,
      "content": content,
      "timepost": timepost,
      "postType" : postType
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
  var table = req.body.table;

  if (userID.length != 0 && timepost.length != 0 && comment.length != 0) {
    db.addComment(userID, timepost, comment, table, function (err, data) {});

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
  var tempList = [];
  db.getAllPosts(req.session.currWall, function (err, data) {
    var contentArr = data.map(obj => obj.content.S);
    var commentsArr = data.map(obj => obj.comments.L);
    var userIDArr = data.map(obj => obj.userID.S);
    var timepostArr = data.map(obj => obj.timepost.S);
    var postTypeArr = data.map(obj => obj.postType.S);
    var likesArr = data.map(function(obj) {
      if(obj.likes == undefined) {
        return "0";
      } else {
        return obj.likes.SS.length+"";
      }
    })
    console.log(likesArr);

    for (let i = 0; i < userIDArr.length; i++) {
      var pointer = {
        "content": contentArr[i],
        "comments": commentsArr[i],
        "userID": userIDArr[i],
        "timepost": timepostArr[i],
        "postType": postTypeArr[i],
        "likes": likesArr[i]
      };
      tempList.push(pointer);
    }
    db.getAllWalls(req.session.currWall, function (err, postsList) {
      var contentArr = postsList.map(obj => obj.content.S);
      var commentsArr = postsList.map(obj => obj.comments.L);
      var userIDArr = postsList.map(obj => obj.sender.S + " to " + obj.receiver.S);
      var timepostArr = postsList.map(obj => obj.timepost.S);
      var postTypeArr = data.map(obj => obj.postType.S);
      var likesArr = data.map(function(obj) {
        if(obj.likes == undefined) {
          return "0";
        } else {
          return obj.likes.SS.length+"";
        }
      })
      console.log(likesArr);

      for (let i = 0; i < userIDArr.length; i++) {
        var pointer = {
          "content": contentArr[i],
          "comments": commentsArr[i],
          "userID": userIDArr[i],
          "timepost": timepostArr[i],
          "postType": postTypeArr[i],
          "likes": likesArr[i]
        };
        tempList.push(pointer);
      }

      db.getFriends(req.session.currWall, function (err, data) {
        var friendsList = [];
        data.forEach(function (r) {
          friendsList.push(r);
        });
        recGetAllWalls(friendsList, tempList, req.session.currWall, 0, function (postsList) {
          if (postsList.length > 1) {
            postsList.sort((a, b) => (a.timepost).localeCompare(b.timepost)).reverse();
          }
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
    callback(recWallsList);
  } else {
    db.getAllWallsAsSender(recFriendsList[counter], sender, function (err, data) {
      var contentArr = data.map(obj => obj.content.S);
      var commentsArr = data.map(obj => obj.comments.L);
      var userIDArr = data.map(obj => obj.sender.S + " to " + obj.receiver.S);
      var timepostArr = data.map(obj => obj.timepost.S);
      var postTypeArr = data.map(obj => obj.postType.S);
      var likesArr = data.map(function(obj) {
        if(obj.likes == undefined) {
          return "0";
        } else {
          return obj.likes.SS.length+"";
        }
      })
      console.log(likesArr);

      for (let i = 0; i < userIDArr.length; i++) {
        var pointer = {
          "content": contentArr[i],
          "comments": commentsArr[i],
          "userID": userIDArr[i],
          "timepost": timepostArr[i],
          "postType": postTypeArr[i],
          "likes": likesArr[i]
        };
        recWallsList.push(pointer);
      }
      counter++;
      recGetAllWalls(recFriendsList, recWallsList, sender, counter, callback);
    });
  }
}

//check if the wall is a friend of the user
var getIsWallAFriend = function (req, res) {
  db.getFriends(req.session.username, function (err, data) {
    if(err) {
      console.log(err);
    }
    var isFriend = {BOOL: false};
    if(req.session.username === req.session.currWall) {
      isFriend = {BOOL: true};
      res.send(isFriend);
    } else {
      data.forEach(function (r) {
        if(r === req.session.currWall) {
          isFriend = {BOOL: true};
          res.send(isFriend);
        }
      })
      if(isFriend.BOOL === false) {
        isFriend = {BOOL: false};
        res.send(isFriend);
      }
    }
  });
}


//create new wall in the db when all inputs exist in posts
var postNewWallAjax = function (req, res) {
  var content = req.body.content;
  var timepost = req.body.timepost;
  var postType = {
    S: "walls"
  };
  if (content.length != 0 && timepost.length != 0) {
    db.createWall(req.session.currWall, req.session.username, content, timepost, function (err, data) { });

    var response = {
      "userID": req.session.username + " to " + req.session.currWall,
      "content": content,
      "timepost": timepost,
      "postType" : postType
    };

    res.send(response);
  } else {
    res.send(null);
  }
}

//get the information about the user for edit
var getEditUserInfoAjax = function (req, res) {
  db.getUserInfo(req.session.username, "username", function (err, data) {
    data.password.S = "";
    res.send(data);
  });
}

//sign in info to the database
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

  res.render('editaccount.ejs', { "check": true });

  db.getInterest(req.session.username, function (err, data) {
    var interestSet = new Set();
    for (let i = 0; i < data.length; i++) {
      interestSet.add(data[i].S);
    }

    recUpdateUser(req.session.username, updateInfoList, updateInfoNameList, 0, function (err, message) {
      if (err) {
        console.log(err);
      }

      var stringify1 = { 
        S: req.body.interest1
      };
      var stringify2 = { 
        S: req.body.interest2
      };
      var stringify3 = { 
        S: req.body.interest3
      };
      db.updateInterest(req.session.username, stringify1, stringify2, stringify3, function (err, data) {
        if (err) {
          console.log(err);
        }
        for (let i = 0; i < data.length; i++) {
          if (!interestSet.has(data[i].S)) {
            var newContent = req.session.username + " is now interested in " + data[i].S;
            var newTimepost = new Date().getTime() + "";
            console.log("run exec");
            exec('mvn exec:java@livy', (err, stdout, stderr) => {
              console.log(err);
              console.log(stdout);
            });
            db.createWall(req.session.username, req.session.username, newContent, newTimepost, function (err, data) { });
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
      counter++;
      recUpdateUser(sessionUser, recUpdateInfoList, recUpdateInfoNameList, counter, callback);
    });
  }
}

//get all usernames from users database
var getAllUsername = function (req, res) {
  db.getAllUsername(function (err, data) {
    if(err) {
      console.log(err);
    }
    res.send(data);
  });
}

//renders visualizer
var getVisualizer = function (req, res) {
  if (!req.session.username) {
    return res.redirect('/')
  }
  res.render('friendvisualizer.ejs', { "check": req.session.isVerified })
}

//ajax: deletes the restaurant data in db
// var postDeleteRestaurantAjax = function (req, res) {
//   var resName = req.body.name;
//   db.deleteRestaurant(resName, function (err, data) { });
//   res.send(resName);
// };

// Send friend request
var sendFriendRequest = function(req, res) {
	var receiver = req.body.receiver;
	if (!req.session.username) {
		res.render('main.ejs', { message: "Not logged in" });
	} else {
		db.addRequest(receiver, req.session.username, function(err, data) {
			if (err) {
				console.log(err);
			}
      res.send({ S: "sent friend request"});
		});
	}
}

// Receiver rejects friend request
var rejectFriendRequest = function(req, res) {
	var sender = req.body.sender;
	if (!req.session.username) {
		res.render('main.ejs', { message: "Not logged in" });
	} else {
		db.deleteRequest(req.session.username, sender, function(err, data) {
			if (err) {
				console.log(err);
			}
      res.send({S: "rejected friend request"});
		});
	}
}

// Receiver accepts friend request. Sender and receiver are both added to each other's friends set
var acceptFriendRequest = function(req, res) {
	var sender = req.body.sender;
	if (!req.session.username) {
		res.render('main.ejs', { message: "Not logged in" });
	} else {
		db.deleteRequest(req.session.username, sender, function(err1, data) {
			if (err1) {console.log(err1);
			} else {
				db.addFriend(req.session.username, sender, function (err2, data) {
					if (err2) {console.log(err2);
					} else {
						db.addFriend(sender, req.session.username, function (err3, data) {
							if (err3) { console.log(err3) }
              res.send({S : "accepted friend request"});
						});
					}
				});
			}
		});
	}
}

//adds likes to the specific post
var addLikesToPost = function (req, res) {
  var userID = req.body.userID;
  var postType = req.body.postType;
  var timepost = req.body.timepost;
  var likedUser = req.session.username;
  
  db.addLike(userID, likedUser, timepost, postType, function(err, data) {
    if (err != null) {
      console.log(err);
    } else {
      var likedNumber = {
        S: data.length
      }
      res.send(likedNumber);
    }
  })
}

//delete friend
var postDeleteFriend = function(req, res){
  var friend = req.body.friend;
  db.deleteFriend(req.session.username, friend, function (err, data) {
    res.send({S: "deleted friend"});
  })
}

//gets friend list
var getFriendList = function(req, res){
  db.getFriends(req.session.username, function (err, data) {
    console.log({L : data});
    res.send({L : data});
  })
}

// TODO Don't forget to add any new functions to this class, so app.js can call them. (The name before the colon is the name you'd use for the function in app.js; the name after the colon is the name the method has here, in this file.)

var routes = {
  get_main: getMain,
  verifyUser: postResultsUser,
  get_signup: getSignup,
  get_logout: getLogout,
  get_creator: getCreator,
  get_chat: getChat,
  get_wall: getWall,
  post_otherWallPageAjax: postOtherWallPageAjax,
  get_determineWallOwner: getDetermineWallOwner,
  get_userInfo: getUserInfo,
  get_edit: getEdit,
  get_news: getNews,
  get_news_search_result: getNewsSearchResult,
  get_news_search: getNewsSearch,
  get_otherwall: getOtherWall,
  reject_friend_request: rejectFriendRequest, 
  accept_friend_request: acceptFriendRequest, 
  send_friend_request: sendFriendRequest, 
  get_isWallAFriend: getIsWallAFriend,

  //NEW
  get_homepage: getHomepage,
  get_homepagePostListAjax: getHomepagePostListAjax,
  get_wallListAjax: getWallListAjax,
  get_editUserInfoAjax: getEditUserInfoAjax,
  get_allUsername: getAllUsername,

  post_newPostAjax: postNewPostAjax,
  post_newCommentAjax: postNewCommentAjax,
  post_newWallAjax: postNewWallAjax,
  post_updateUser: postUpdateUser,
  post_addLikesToPost: addLikesToPost,
  

  post_newAccount: postNewAccount,
  get_friend_visualizer: getVisualizer,
  post_deleteFriend: postDeleteFriend,
  get_friendList: getFriendList

  //post_newRestaurant : postNewRestaurant
};

module.exports = routes;