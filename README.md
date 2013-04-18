Prozess
=======
[![Build
Status](https://secure.travis-ci.org/cainus/Prozess.png?branch=master)](http://travis-ci.org/cainus/Prozess)
[![Coverage Status](https://coveralls.io/repos/cainus/Prozess/badge.png?branch=master)](https://coveralls.io/r/cainus/Prozess)

Prozess is a Kafka library for node.js
This fork is wrote for Kafka version  0.8, still working on progress.

v0.8's protocol changes a log, so this client's APIs has changed against old version forked from cainus's repo, see examples below:

[Kafka](http://incubator.apache.org/kafka/index.html) is a persistent, efficient, distributed publish/subscribe messaging system.

There are two low-level clients: The Producer and the Consumer:

##Producer example:

```javascript
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
```

##Consumer example:

```javascript
var Consumer = require('./lib/Consumer');


options = {host : 'test.kafka.rome.cluster.sina.com.cn', topic : 'topic', partition : 0, offset : 0};
var consumer = new Consumer(options);
consumer.connect(function(err){
  if (err){
    console.log("Error: " + err);
    throw "could not connect to Kafka";
  }
  console.log("connected!!");  
  console.log("consuming: " + consumer.topic);
  
  consumer.on('message', function(messages){
    messages.forEach(function(m){
      console.log(m.payload.toString());
    });
    setTimeout(consumer.consume.bind(consumer),2000);
  });
  consumer.on('error', function(error){
    console.log(error);
  });

  consumer.on('again', function(){
    setTimeout(consumer.consume.bind(consumer),2000);
  });
  consumer.consume();
    
});

```

A `Consumer` can be constructed with the following options (default values as
shown below):

```javascript
var options = {
  topic: 'test',
  partition: 0,
  host: 'localhost',
  port: 9092,
  offset: null, // Number, String or BigNum
};
```

##Installation:

     npm install prozess

##Checkout the code and run the tests:

     $ git clone https://github.com/cainus/Prozess.git
     $ cd Prozess ; make test-cov && open coverage.html


##Kafka Compatability matrix:

<table>
  <tr>
    <td>Kafka 0.8 from git</td><td>Partial Supported</td>
  <tr>
  <tr>
    <td>Kafka 0.7.2 Release</td><td>Supported</td>
  <tr>
    <td>Kafka 0.7.1 Release</td><td>Supported</td>
  <tr>
    <td>Kafka 0.7.0 Release</td><td>Supported</td>
  <tr>
    <td>kafka-0.6</td><td>Consumer-only support.</td>
  <tr>
    <td>kafka-0.05</td><td>Not Supported</td>
</table>

Versions taken from http://incubator.apache.org/kafka/downloads.html
