// Example that shows how to plot synthetic points
// https://openlayers.org/en/latest/examples/synthetic-points.html

// mongoexport --db yelp_db --collection business_collection --out sample_business.json --limit 1000 --jsonArray
import data from './data/businesswithsentiment.json';// assert {type: 'JSON'};

import './style.css';
import { Map, View } from 'ol';
import VectorLayer from 'ol/layer/Vector';
import VectorSource from 'ol/source/Vector';
import Feature from 'ol/Feature';
import TileLayer from 'ol/layer/Tile';
import { Point } from 'ol/geom';
import OSM from 'ol/source/OSM';
import { fromLonLat } from 'ol/proj';
import { Circle as CircleStyle, Fill, Style } from 'ol/style';

const count = 1000;
const features = new Array(count);
for (let i = 0; i < count; i++) {

  // We have to convert from lon, lat to X, Y
  let proj = fromLonLat([data[i].longitude, data[i].latitude])

  features[i] = new Feature({
    'geometry': new Point([
      proj[0],
      proj[1]
    ]),
    'name': data[i].name,
    'addr': data[i].address + ', ' + data[i].city + ', ' + data[i].state + ', ' + data[i].postal_code,
    'rating': data[i].stars,
    'sentiment': data[i].predicted_sentiment
  });
}

const vectorLayer = new VectorLayer({
  source: new VectorSource({
    features: features,
    wrapX: false
  }),
  style: function (feature) {
    return new Style({
      image: new CircleStyle({
        radius: 5,
        // Color-code points based on rating
        fill: new Fill({ color: 'hsla(' + (feature.values_.rating * 24) + ', 100%, 50%, 0.8)' })
      })
    })
  }
});

const map = new Map({
  target: 'map',
  layers: [
    new TileLayer({
      source: new OSM()
    }),
    vectorLayer
  ],
  view: new View({

    // Center on the continental US by default
    center: [-10783760.540406741, 5896094.054516814],
    zoom: 3.32
  })
});

// When the user clicks on a point, display additional information on the side
map.on("singleclick", function (evt) {

  var p = evt.pixel;
  var feature = map.forEachFeatureAtPixel(p, function (feature) {
    return feature;
  });
  if (feature) {

    // Populate the table on the right with business info
    document.getElementById("business-name").innerHTML = feature.get("name");
    document.getElementById("business-addr").innerHTML = feature.get("addr");
    document.getElementById("business-rating").innerHTML = feature.get("rating");
    document.getElementById("business-sentiment").innerHTML = feature.get("sentiment");
  }
})