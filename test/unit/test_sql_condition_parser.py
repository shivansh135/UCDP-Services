from tracardi.service.storage.persistence_service import SqlSearchQueryParser


# def test_parser_between():
#     parser = SqlSearchQueryParser()
#     # q = parser.parse("interests.pricing > 0 and interests.pricing< 100")
#     q = parser.parse("interests.pricing between 0 and 100")
#     assert q == {'range': {'interests.pricing': {'gte': 0, 'lte': 100}}}
#
def test_parser_equals():
    parser = SqlSearchQueryParser()
    # q = parser.parse("interests.pricing > 0 and interests.pricing< 100")
    q = parser.parse('traits.other =  "A"')
    assert q == {'bool': {'should': [{'match': {'traits.other': 'A'}}, {'term': {'traits.other.keyword': {'value': 'A'}}}]}}

# def test_parser_not_equals():
#     parser = SqlSearchQueryParser()
#     # q = parser.parse("interests.pricing > 0 and interests.pricing< 100")
#     q = parser.parse('traits.other.a != "A"')
#     print(q)


def test_parser_exact_match():
    parser = SqlSearchQueryParser()
    q = parser.parse('traits.other.a == "A"')
    assert q == {'term': {'traits.other.a': {'value': 'A'}}}
    q = parser.parse('traits.other.a is "A"')
    assert q == {'term': {'traits.other.a': {'value': 'A'}}}
    q = parser.parse('traits.other.a is ["A"]')
    print(q)


# def test_parser_fulltext_match():
#     parser = SqlSearchQueryParser()
#     # q = parser.parse("interests.pricing > 0 and interests.pricing< 100")
#     q = parser.parse('traits.other.a ~ "A"')
#     print(q)
#     q = parser.parse('traits.other.a match "A"')
#     print(q)