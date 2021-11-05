from pipeline.app.extract import RFPrototypeJSONExtractor as rf
values = rf.JSONExtractor()

def test_get_keys():
    get_values = values.get_keys
    assert type(get_values) is list
    assert len(get_values) >= 3105
    assert 'Talent/10385.json' in get_values




# def count(x):
#     count = 0
#     for i in x:
#         count += 1
#     return count

# print(count(values.get_keys))
