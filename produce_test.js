var Producer = require('./lib/Producer');

var producer = new Producer('topic', {host : 'test.kafka.rome.cluster.sina.com.cn', 'requireAcks': 1});

producer.on('error', function(err,res){
  console.log("error: ", err);
  console.log("res: ", JSON.stringify(res));
});


producer.on('connect', function(){
	producer.send(['123','456','789']);
});

producer.on('sent', function(error, res){
	console.log(JSON.stringify(res));
});

producer.connect();