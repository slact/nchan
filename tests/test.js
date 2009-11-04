//if using firefox, don't for get to set network.http.max-connections-per-server to something rather large
//also network.http.max-persistent-connections-per-server
//and network.http.max-connections
function broadcast(n, message) {
	var ch="foo" + Math.random();
	console.log(ch);
	for(var i=0; i<n; i++) {
		new Request({
			url: "/broadcast/sub?channel=" + ch + "&i=" + i,
			method: 'GET'
		}).addEvent('complete', function() {
			console.log(this);
		}).send();
	}
	setTimeout(function() {
		new Request({
			url: "/broadcast/pub?channel=" + ch,
			method: 'POST', 
			complete: function(a, b, c) {
				console.log(this, a, b, c);
			}
		}).send();
	}, 2000);
}