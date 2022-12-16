var db = require('../models/database.js');

var getVisualizer = function (req, res) {
	if (!req.session.username) {
 		return res.redirect('/');
	}
  	res.render('friendvisualizer.ejs');
}

// Gets a list of friends and users with the same affiliation for the visualizer
var getFriendsVisualize = function(req, res) {
  	if (!req.session.username) {
    	return res.redirect('/');
  	} else {
		db.getFriends(req.session.username, function (err1, data1) {
			if (err1) {
				console.log(err1);
			} else {
				var node = {"id": req.session.username, 
				            "name": "Me", 
				            "children": []};
				data1.forEach(function (r) {
					node.children.push({
						"id": r, 
			            "name": r, 
			            "children": [],
					});
				});
				db.getUserAffiliation(req.session.username, function (err2, data2) {
					if (err2) {
						console.log(err2);
					} else {
						db.getAffiliations(data2, function (err3, data3) {
							if (err3) {
								console.log(err3);
							} else {
								data3.forEach(function (d3) {
									node.children.push({
										"id": d3, 
							            "name": d3, 
							            "children": [], 
									});
								});
								res.send(node);
							}
						});
					}
				});
			}
		});
	}
}

// When user clicks on a node, that user's friends will appear
var expandGraph = function(req, res) {
  	if (!req.session.username) {
    	return res.redirect('/');
  	} else {
		var clickedFriend = req.params.user;	
		db.getFriends(clickedFriend, function(err, data) {
			if (err) {
				console.log(err);
			} else {
				var node = {
					"id": clickedFriend, 
		            "name": clickedFriend, 
		            "children": [], 
				}
				//console.log("here: "+ JSON.stringify(data));
				data.forEach(function (r) {
					node.children.push({
						"id": r, 
			            "name": r, 
			            "children": [], 
					});
				});
				db.getUserAffiliation(req.session.username, function (err2, data2) {
					if (err2) {
						console.log(err2);
					} else {
						db.getAffiliations(data2, function (err3, data3) {
							if (err3) {
								console.log(err3);
							} else {
								data3.forEach(function (d3) {
									node.children.push({
										"id": d3, 
							            "name": d3, 
							            "children": [], 
									});
								});
								res.send(node);
							}
						});
					}
				});
			}
		});
	}
}

var routes = {
  get_friend_visualizer: getVisualizer,
  get_friends: getFriendsVisualize,
  get_more_friends: expandGraph,
};

module.exports = routes;
