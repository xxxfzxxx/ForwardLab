from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import TransportError
import json
import os
import time
start_time = time.time()

class ElasticObj:
    def __init__(self, index_name, index_type):
        """
        index_name: 索引名称
        index_type: 索引类型
        """
        self.index_name = index_name
        self.index_type = index_type
        # 无用户名密码状态
        self.es = Elasticsearch(["localhost"], port=9200)
        # 用户名密码状态
        # self.es = Elasticsearch([ip],http_auth=("elastic", "password"),port=9200)

    def create_index(self):
        # 创建映射
        print("creating index...")
        index_mappings = {

            "mappings": {
                self.index_type: {
                    "properties": {
                        "id": {  
                            "type": "keyword",
                            "index": "false"
                        },
                        "title": {  
                            "type": "text",
                            "analyzer": "snowball"
                        },
                        "authors": {  
                            "type": "nested",
                            "properties": {
                                "name": {"type": "text", "index": "false"},
                                "org": {"type": "text", "index": "false"},
                                "org_id": {"type": "text", "index": "false"},
                                "id": {"type": "text", "index": "false"}
                            }
                        },
                        
                        "venue": { 
                            "properties": {
                                "id" : {"type": "text", "index": "false"},
                                "name": {"type": "text", "index": "false"}
                            }
                        },
                        "year": { 
                            "type": "integer",
                            "index": "false"
                        },
                        "keywords": {  
                            "type": "text",
                            "index": "false"
                        },
                        "fos": {  
                            "type": "nested",
                            "properties": {
                                "name": {"type": "text", "index": "false"},
                                "w": {"type": "float", "index": "false"}
                            }
                        },
                        "n_citation": {  
                            "type": "integer",
                            "index": "false"
                        },
                        "page_start": {  
                            "type": "integer",
                            "index": "false"
                        },
                        "page_end": { 
                            "type": "integer",
                            "index": "false"
                        },
                        "doc_type": {
                            "type": "text",
                            "index": "false"
                        },
                        "lang": { 
                            "type": "text",
                            "index": "false"
                        },
                        "publisher": {
                            "type": "text",
                            "index": "false"
                        },
                        "volume": { 
                            "type": "text",
                            "index": "false"
                        },
                        "issue": { 
                            "type": "text",
                            "index": "false"
                        },
                        "issn": {  
                            "type": "text",
                            "index": "false"
                        },
                        "isbn": { 
                            "type": "text",
                            "index": "false"
                        },
                        "doi": { 
                            "type": "text",
                            "index": "false"
                        },
                        "pdf": { 
                            "type": "text",
                            "index": "false"
                        },
                        "url": {  
                            "type": "text",
                            "index": "false"
                        },
                        "abstract": { 
                            "type": "text",
                            "analyzer": "snowball"
                        }
                    }
                }
            }
        }

        try:
            print("try...")
            self.es.indices.create(
                index=self.index_name,
                body=index_mappings, ignore=[400, 404])
        except TransportError as e:
            print("except")
            # ignore already existing index
            if e.error == "index_already_exists_exception":
                pass
            else:
                raise

    # 插入数据
    def insert_data(self, inputfile):
        time_start = time.time()
        print("inserting data...")
        path1=os.path.abspath(".") 
        f = open(path1+inputfile, "r", encoding="UTF-8")

        ACTIONS = []
        i = 1
        bulk_num = 2000
        for list_line in f.readlines():
            action = {
                "_index": self.index_name,
                "_type": self.index_type,
                "_id": i,  # _id 也可以默认生成，不赋值
                "_source": json.loads(list_line)
            }
            i += 1
            ACTIONS.append(action)
            # 批量处理
            
            if len(ACTIONS) == bulk_num:
                print("index data", int(i))
                success, _ = bulk(client=self.es, actions=ACTIONS, raise_on_error=False)
                del ACTIONS[0:len(ACTIONS)]
                print("complete del")

        if len(ACTIONS) > 0:
            success, _ = bulk(client=self.es, actions=ACTIONS, raise_on_error=False)
            del ACTIONS[0:len(ACTIONS)]
            print("Performed %d actions" % success)

        f.close()
        print("177")


if __name__ == "__main__":
    
    obj = ElasticObj("academic", "article")
    obj.create_index()
    obj.insert_data("/pagerank_result.json")
    print("here")
    
time_end = time.time()
print("total time {} s".format(time_end-start_time))  