var Producer = require('./lib/Producer');

var producer = new Producer('test', {host : '10.75.15.235', 'requireAcks': 1});
producer.connect()
producer.on('error', function(err){
  console.log("error: ", err);
});

console.log("producing for ", producer.topic);
console.log("sending...");
producer.send(['123','456','789'], function(err, res){
  console.log("Error: ", err);
  console.log("Response: ", JSON.stringify(res));
  producer.connection.end();
});
