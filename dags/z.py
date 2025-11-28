# import json

# from services.models_etl import ModelsExtractor, ModelsTransformer


# extractor = ModelsExtractor(limit=10000)
# transformer = ModelsTransformer()

# models = extractor.extract()

# with open('res.json', 'w') as f:
#     for model in models:
#         transformed = transformer.transform(model.__dict__)
#         f.write(json.dumps(transformed, ensure_ascii=False))
#         f.write('\n')

import json

with open('res.json', 'r') as f:
    for line in f:
        j = json.loads(line)
        if type(j['trending_score']) != int:
            print(j['trending_score'])
