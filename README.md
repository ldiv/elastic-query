
ElasticQuery
=======

Simplified query syntax for Elasticsearch


Usage
-----

From client code:

	from elastic_query import ElasticQuery 
    ...
    
    URL = '<elasticsearch instance url>'
    INDEX = '<index name>'
    TYPE = '<type name>'
    query = '<query>'
    
	eq = ElasticQuery(URL, INDEX, TYPE)
    eq.search(query)
    eq.print_response_summary([<field_name>])
    eq.print_query()


Example
-------

Simplified query:

	query = "Long_Desc~='almond butter' or (Long_Desc~=almond and food_group~='Nut and Seed Products')"

Translated to Elasticsearch Query DSL:	

    {
      "query": {
        "bool": {
          "should": [
            {
              "match_phrase": {
                "Long_Desc": "almond butter"
              }
            },
            {
              "bool": {
                "must": [
                  {
                    "match": {
                      "Long_Desc": "almond"
                    }
                  },
                  {
                    "match_phrase": {
                      "food_group": "Nut and Seed Products"
                    }
                  }
                ]
              }
            }
          ]
        }
      },
      "size": 10
    }


License
-------

MIT License

