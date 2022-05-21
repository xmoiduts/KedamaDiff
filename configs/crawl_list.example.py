#备注: 给每个地图单开一个class非我所愿，我希望使用`.`来访问地图对象中的元素。
# Usage: fill in maps and left other classes 'as is'

maps =  {
    "v1_daytime": {
        "crawl_zones": "[((0, -8), 10, 10)]",
        "enable_crawl": False,
        "map_domain": "https://map.example.com/kedama",
        "map_name": "v1", # In the long run, map's URL name may change, ...
        "map_rotation": "tl", # preserved for mapcrafter
        "map_savename": "v1", # ... while we can keep a consistent name for where we save the map, 'savename' are meant to be unchanged.
        "max_crawl_workers": 4, # 2-8 workers for this single-threaded aiohttp script is enough.
        "note": "[((0, -8), 80, 40)]", # free text, not used by the script executable.
        "target_depth": -3,
        "ancestor_probing_level": -3 # how many levels will crawler probe the existence for a given tile path? ...
        #... 0 if vacant, -n means each probe determines whether to skip accessing 2**n image tiles.
    },
    "v2_daytime": {
        "crawl_zones": "[((0, -8), 80, 40)]",
        "enable_crawl": False,
        "latest_renderer": "Mapcrafter",
        "map_domain": "https://map.example.com/kedama",
        "map_name": "v2",
        "map_rotation": "tl",
        #"map_savename": "v2_daytime",
        "max_crawl_workers": 4,
        "target_depth": -3
    },
    "v4_daytime": {
        "crawl_zones": "[((8, 520), 10, 10)]",
        "enable_crawl": True,
        "map_domain": "https://map.example.com/kedama",
        "map_name": "v4",
        "map_rotation": "tl",
        "max_crawl_workers": 8,
        "note": "[((0, 0), 140, 70)]",
        "target_depth": -3
    }
}

# --- Don't touch below ---

class dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for k, v in self.items():
            if isinstance(v, dict):
                self[k] = dotdict(v)    


CrawlList = dotdict(maps)
