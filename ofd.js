var http = require('http');
var mysql = require('mysql');
var config = require('./config.js');

var d = require('domain').create();

var pool  = mysql.createPool({
  host     : config.db_host,
  user     : config.db_user,
  password : config.db_password,
  database : config.db_database
});


d.on('error', function(err){
	if (err) {
		console.error('UNKNOWN ERROR: ' + err);
	}
});

d.run(function(){
	var x = function(){	
		pool.getConnection(function(err, connection) {
			if (err) {
				console.error('FATAL ERROR, CONNECTION ERROR: ' + err.stack);
				setTimeout(x, 2000);
				return;
			}
			console.log('Connected to ' + config.db_host);
			getNext(connection);		
		});			
	};
	
	function getNext(connection){
		connection.query('CALL EventPopFromQueue(1)', [], function(err, results){
			if(err){
				console.log('error');
				connection.release();
				setTimeout(x, 2000);
				return;
			}
			if (results[0][0].uspeh == 0){
				console.log('done');
				connection.release();
				setTimeout(x, 2000);
				return;
			}else{
				console.log('processing message');
				
				var event_info = results[0][0];

				var post_data = '';
				post_data += '{';
				post_data += '"action":"' + event_info.action + '",';
				post_data += '"sub_action":"' + event_info.sub_action + '",';
				post_data +=   '"integ_params": {';
				post_data +=     '"event":{';
				post_data +=       '"event_id":"' + event_info.event_id + '",';
				post_data +=       '"outside_ref":"' + event_info.event_outside_ref + '",'
				post_data +=       '"kickoff_date":"' + event_info.kickoff_date + '",';
				post_data +=       '"kickoff_time":"' + event_info.kickoff_time + '",';
				post_data +=       '"status":"' + event_info.status + '"';
				post_data +=     '},';
				post_data +=     '"competition":{';
				post_data +=       '"competition_id":"' + event_info.competition_id + '",';
				post_data +=       '"competition_name":"' + event_info.competition_name + '",';
				post_data +=       '"group_id":"' + event_info.group_id + '",';
				post_data +=       '"group_name":"' + event_info.group_name + '"';
				post_data +=     '},';
				post_data +=     '"trader":{';
				post_data +=       '"trader_id":"' + event_info.trader_id + '",';
				post_data +=       '"username":"' + event_info.username + '",';
				post_data +=       '"outside_ref":"' + event_info.outside_ref + '"';
				post_data +=     '},';
				post_data +=     '"home_team":{';
				post_data +=       '"team_id":"' + event_info.home_team_id + '",';
				post_data +=       '"team_name":"' + event_info.home_team_name + '",';
				post_data +=       '"outside_ref":"' + event_info.home_team_outside_ref + '"';
				post_data +=     '},';
				post_data +=     '"away_team":{';
				post_data +=       '"team_id":"' + event_info.away_team_id + '",';
				post_data +=       '"team_name":"' + event_info.away_team_name + '",';
				post_data +=       '"outside_ref":"' + event_info.away_team_outside_ref + '"';					
				post_data +=     '},';
				post_data +=     '"markets":{';
				var prvi_market = true;
				var prva_selekcija = true;
				var market = '';
				for(var i = 0; i < results[1].length; i++){
					if (market !== results[1][i].market_key){
						market = results[1][i].market_key;
						prva_selekcija = true;
						if (prvi_market == false){
							post_data +=       '}';
							post_data +=     '},';								
						}
						prvi_market = false;
						post_data +=     '"' + results[1][i].market_key + '":{';
						post_data +=       '"name":"' + results[1][i].market_name + '",';
						post_data +=       '"general_name":"' + results[1][i].market_general_name + '",';
						post_data +=       '"has_lines":' + results[1][i].market_has_lines + ',';
						post_data +=       '"max_lines":' + results[1][i].market_max_lines + ',';
						post_data +=       '"code":"' + results[1][i].market_code + '",';
						post_data +=       '"inplay":' + results[1][i].market_inplay + ',';
						post_data +=       '"ordernum":' + results[1][i].market_ordernum + ',';
						post_data +=       '"group":"' + results[1][i].market_group + '",';
						post_data +=       '"min_price":' + results[1][i].market_min_price + ',';
						post_data +=       '"max_price":' + results[1][i].market_max_price + ',';
						post_data +=       '"selections":{';
						// post_data +=       '}';
						// post_data +=     '}';	 						
					}
					if (prva_selekcija == false) 
						post_data += ',';					
					post_data += '"' + results[1][i].selection_key + '": {';
					post_data +=   '"name":"' + results[1][i].selection_name + '",';
					post_data +=   '"code":"' + results[1][i].selection_code + '",';
					post_data +=   '"lob_code":"' + results[1][i].lob_code + '",';
					post_data +=   '"ordernum": ' + results[1][i].selection_ordernum + ',';
					post_data +=   '"odds":{';
					post_data +=     '"dec": ' + results[1][i].odd_dec + ',';
					post_data +=     '"en_up": ' + results[1][i].odd_en_up + ',';
					post_data +=     '"en_down": ' + results[1][i].odd_en_down + '';
					post_data +=   '}';
					post_data += '}';
					prva_selekcija = false
				};			
				post_data +=         '}'; // last selection
				post_data +=       '}';	  // selections	
				post_data +=     '}';     // markets
				post_data +=   '}';       // integ_params
				post_data += '}';         // 

				var post_options = {
					host: config.subscriber_host,
					port: config.subscriber_port,
					path: config.subscriber_path,
					method: 'POST',
					headers: {
						'Content-Type': 'application/x-www-form-urlencoded',
						'Content-Length':  Buffer.byteLength(post_data, 'utf8')	
					}
				};			
				
				var post_req = http.request(post_options, function(res) {				
				});
				post_req.write(post_data);
				post_req.end();
				console.log(post_data);
				
				getNext(connection);
			}
		});		
	};
	
	x();
});