var overviewerConfig = {
    "worlds": [
        "Azeroth",
		"Outland"
    ], 
    "tilesets": [
        {
            "spawn": [-2410, 64, 6763], 
            "isOverlay": false, 
            "last_rendertime": 1383588879, 
            "name": "Azeroth", 
            "poititle": "Markers", 
            "north_direction": 1, 
            "minZoom": 0, 
            "bgcolor": "#1a1a1a", 
            "zoomLevels": 11, 
            "base": ["http://static.overviewer.org/renders/"],
            "imgextension": "png", 
            "defaultZoom": 2, 
            "world": "Azeroth", 
            "maxZoom": 11, 
            "path": "Azeroth", 
            "showlocationmarker": true
        },
        {
            "spawn": [4614, 64, 4479], 
            "isOverlay": false, 
            "last_rendertime": 1370743963, 
            "name": "Outland", 
            "north_direction": 1, 
            "minZoom": 0, 
            "bgcolor": "#1a1a1a", 
            "zoomLevels": 9, 
            "base": ["http://static-cf.overviewer.org/renders/"],
            "imgextension": "png", 
            "defaultZoom": 3, 
            "world": "Outland", 
            "maxZoom": 9, 
            "path": "Outland"
        }
    ], 
    "CONST": {
        "mapDivId": "mcmap", 
        "UPPERLEFT": 0, 
        "tileSize": 384, 
        "UPPERRIGHT": 1, 
        "image": {
            "defaultMarker": "signpost.png", 
            "queryMarker": "https://google-maps-icons.googlecode.com/files/regroup.png", 
            "signMarker": "signpost_icon.png", 
            "spawnMarker": "https://google-maps-icons.googlecode.com/files/home.png", 
            "bedMarker": "bed.png"
        }, 
        "LOWERRIGHT": 2, 
        "LOWERLEFT": 3, 
        "regionStrokeWeight": 2
    }, 
    "map": {
        "debug": true, 
        "north_direction": "lower-left", 
        "controls": {
            "spawn": true, 
            "mapType": true, 
            "compass": true, 
            "coordsBox": true, 
            "overlays": true, 
            "searchBox": true, 
            "zoom": true, 
            "pan": true
        }, 
        "center": [
            -314, 
            67, 
            94
        ], 
        "cacheTag": "1383601862"
    }
};
