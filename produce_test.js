var Producer = require('./lib/Producer');

var producer = new Producer('test', {host : '10.75.15.235'});
producer.connect()
producer.on('error', function(err){
  console.log("error: ", err);
});

console.log("producing for ", producer.topic);
console.log("sending...");
producer.send('123', function(err){
  console.log("send error: ", err);
});
