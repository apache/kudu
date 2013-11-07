// Copyright (c) 2013, Cloudera, inc.

var gauges = [];
var last_data = [];
var last_update = Date.now();
var num_gauges = 0;

function createGauge(name, label, min, max) {
  var config = {
    size: 250,
    label: "",
    min: undefined != min ? min : 0,
    max: undefined != max ? max : 10000,
    minorTicks: 5
  }

  var range = config.max - config.min;
  config.yellowZones = [{ from: config.min + range*0.75, to: config.min + range*0.9 }];
  config.redZones = [{ from: config.min + range*0.9, to: config.max }];

  var span_id = 'gauge_' + num_gauges++;
  var div = d3.select('#gauges').append('div');
  div.attr('class', 'gauge-container');

  var name_elem = div.append('div');
  name_elem.attr('class', 'metric-name');
  name_elem.text(name);

  var label_elem = div.append('div');
  label_elem.attr('class', 'metric-label');
  label_elem.text(label);

  div.append('div').property('id', span_id);

  gauges[name] = new Gauge(span_id, config);
  gauges[name].render();
}

function updateGauges(json, error) {
  if (error) return console.warn(error);
  if (! json) {
    // Unclear why, but sometimes we seem to get a null
    // here.
    console.warn("null JSON response");
    setTimeout(function() {
      d3.json("/jsonmetricz", updateGauges);
    }, 1000);
    return;
  }

  var now = Date.now();
  var delta_time = now - last_update;

  json['metrics'].forEach(function(m) {
    var name = m['name'];
    if (!(name in gauges)) {
      createGauge(name, m['description']);
    }
    var g = gauges[name];

    if (name in last_data) {
      var delta_value = m.value - last_data[name];
      var rate = delta_value / delta_time * 1000;

      g.redraw(rate);
    }

    last_data[name] = m.value;
  });

  last_update = now;

  setTimeout(function() {
    d3.json("/jsonmetricz", updateGauges);
  }, 300);
}

function initialize() {
  d3.json("/jsonmetricz", updateGauges);
}
