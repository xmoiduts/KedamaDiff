#备注: 给每个地图单开一个class非我所愿，我希望使用`.`来访问地图对象中的元素。
# Usage: fill in maps and left other values 'as is'

maps =  {
    "v1_daytime": {
        "crawl_zones": "[((0, -8), 10, 10)]",
        "enable_crawl": False,
        "last_total_depth": 10,
        "latest_renderer": "Mapcrafter",
        "map_domain": "https://map.example.com/kedama",
        "map_name": "v1",
        "map_rotation": "tl",
        "map_savename": "v1_daytime",
        "max_crawl_threads": 16,
        "note": "[((0, -8), 80, 40)]",
        "target_depth": -3
    },
    "v2_daytime": {
        "crawl_zones": "[((0, -8), 80, 40)]",
        "enable_crawl": False,
        "last_total_depth": 13,
        "latest_renderer": "Mapcrafter",
        "map_domain": "https://map.example.com/kedama",
        "map_name": "v2",
        "map_rotation": "tl",
        "map_savename": "v2_daytime",
        "max_crawl_threads": 16,
        "target_depth": -3
    },
    "v4_daytime": {
        "crawl_zones": "[((8, 520), 10, 10)]",
        "enable_crawl": True,
        "last_total_depth": 11,
        "latest_renderer": "Mapcrafter",
        "map_domain": "https://map.example.com/kedama",
        "map_name": "v4",
        "map_rotation": "tl",
        "max_crawl_threads": 24,
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
