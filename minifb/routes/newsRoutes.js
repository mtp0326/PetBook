var db = require('../models/database.js');
var newsdb = require('../models/newsDB.js');

//render news page
var getNewsPage = function (req, res) {
	if (!req.session.username) {
 		return res.redirect('/');
	}
  	res.render('news.ejs');
}


// when interested is added, add recommendations to user db
// when user goes to news page, call the recommended artices
// user can like news
// 
//search news - HW2


// Get user's recommended articles
var getRecommended = function (req, res) {
	var articleList = [];

	if (!req.session.username) {
		return res.redirect('/')
	} else {
		//retrieves user's article titles in a string set
		newsdb.getTitles(req.session.username, function (err1, data1) {
			if (err1) {
				console.log(err1);
			} else {
				console.log(data1);
				// data1: list of all the user's chatrooms ids
				data1.forEach(function (r) {
					console.log("get recommended articles of this title: " + r);
					newsdb.getArticle(r, function (err2, data2) {
						if (err2) {
							console.log(err2);
						} else {
							articleList.push(data2);
							if (articleList.length == 10) {
								// list of data2
								res.json(articleList);
							}
						}
					})
				});
			}
		});
	}
};

var addLike = function(req, res) {
	var userID = req.session.username;
	var likedNews = req.body.likedNews;
	
	newsdb.addLike(userID, likedNews, function(err, data) {
		if (err) {
			console.log(err);
		} else {
			var currLike = {S: data};
			res.send(currLike);
		}
	})
}

/*
//newsSearch
//talks: search through inverted table
var getNewsSearch = function(req, res) {	
  
    var docClient = new AWS.DynamoDB.DocumentClient();
    var talkids = [];
    var promisesInverted = [];
    var promisesTalks = [];
    var results = [];
    var keywords = request.query.keyword.split(" ");
  
    //loop for each keyword to fetch talk ids of the keywords
    for (let i = 0; i < keywords.length; i++) {

		//stem
		let modifiedKeyword = (keywords[i]).toLowerCase();
		if (['a','all','any','but','the'].includes(modifiedKeyword)) {
			modifiedKeyword = "Thisisastopword";
		}
		modifiedKeyword = stemmer(modifiedKeyword);
	  
	  	var params = {
			KeyConditionExpression: 'keyword = :k',
			ExpressionAttributeValues:{':k' : modifiedKeyword},
			TableName: 'newsKeyword',
			Limit: 10,
	  	};
  	
	  	//create and push promises for each keyword to query on inverted
	  	var promise = docClient.query(params).promise();
	  	promisesInverted.push(promise);
    }
  
    //query inverted on all the keywords
    Promise.all(promisesInverted).then(success => {
	
		//results of successful promise (Promise #1)
	    for (let i = 0; i < success.length; i++) {
		    //console.log("Success", data[i]);
		    success[i].Items.forEach(function(item) {
			
			    //limit the searched talk ids to 15
			  	if (!talkids.includes(item.inxid) && talkids.length < 15){
					talkids.push(item.inxid);
				}
			});
		};
		
		//for each talk id, create and push promise to query on ted_talks
		for (let i = 0; i < talkids.length; i++) {
			var param = {
				KeyConditionExpression: 'talk_id = :t',
				ExpressionAttributeValues:{':t' : talkids[i]},
				TableName: 'ted_talks'
		  	};
		  	var promise = docClient.query(param).promise();
			promisesTalks.push(promise);
		}
		
		//query ted_talks on all the talk ids
		Promise.all(promisesTalks)
			.then(
				//results of successful promise (Promise #2)
				success => {
					for (let i = 0; i < talkids.length; i++) {
						var result = success[i].Items;
						//console.log(i + "th result: " + result[0]['topics']);
						
						//parse
						result[0]['topics'] = JSON5.parse(result[0]['topics']);
						
						//For the JSON5 parsing error 's, M'B, 've 
						if ((result[0]['related_talks']).includes("'s")) {
							result[0]['related_talks'] = (result[0]['related_talks']).replace("'s", "s");
						} else if ((result[0]['related_talks']).includes("M'B")) {
							result[0]['related_talks'] = (result[0]['related_talks']).replace("M'B", "MB");
						} else if ((result[0]['related_talks']).includes("'ve")) {
							result[0]['related_talks'] = (result[0]['related_talks']).replace("'ve", "ve");
						}
						console.log(result[0]['related_talks']);
						
						result[0]['related_talks'] = 
							JSON5.parse(result[0]['related_talks'].replace(/[0-9]+/g, x => '\"'+x+'\"'));
						results.push(result[0]);
					}
					//render using results.pug file
					response.render("results", { "search": keywords, "results": results });
				})
			.catch(err => {
				//function called on error (Promise #2)
				console.log("Error", err);
			})},
		)
		.catch(
			//function called on error (Promise #1)
			err => {console.log("Error", err)});
});


*/
var routes = {
	get_news_page: getNewsPage,
	get_recommended: getRecommended,
	add_like: addLike,
	//get_search: getNewsSearch,
};

module.exports = routes;