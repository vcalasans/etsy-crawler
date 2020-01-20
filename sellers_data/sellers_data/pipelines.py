import pprint as pprint

class SellersDataPipeline(object):
    def process_item(self, item, spider):
        pprint(item)
        return item
