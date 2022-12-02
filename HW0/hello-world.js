var http = require('http');
http.createServer(function (req, res) {
  res.writeHead(200, {'Content-Type': 'text/plain'});
  // TODO: Change the text below to 'Hello World - this is <yourName> (<yourSEASlogin>)'
  // Example: 'Hello World - this is Andreas Haeberlen (ahae)'
  res.end('Hello World - this is Cindy Wei\n');
}).listen(8080, '127.0.0.1');
console.log('Server running at http://127.0.0.1:8080/');
