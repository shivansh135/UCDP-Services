from tracardi.service.storage.persistence_service import SqlSearchQueryParser


def test_parser_between():
    parser = SqlSearchQueryParser()
    q = parser.parse("interests.pricing between 0 and 100")
    assert q == {'range': {'interests.pricing': {'gte': 0, 'lte': 100}}}

def test_parser_equals():
    parser = SqlSearchQueryParser()
    q = parser.parse('traits.other =  "A"')
    assert q == {'bool': {'should': [
        {'match': {'traits.other': 'A'}},
        {'term': {'traits.other': {'value': 'A'}}},
        {'term': {'traits.other.keyword': {'value': 'A'}}}
    ]}}
    q = parser.parse('traits.other =  "A*"')
    assert q == {'bool': {'should': [
        {'match': {'traits.other': 'A*'}}, {'wildcard': {'traits.other': {'value': 'A*'}}},
        {'wildcard': {'traits.other.keyword': {'value': 'A*'}}}
    ]}}


def test_parser_not_equals():
    parser = SqlSearchQueryParser()
    q = parser.parse('traits.other.a != "A"')
    assert q == {'bool': {'must_not': {'term': {'traits.other.a': {'value': 'A'}}}}}
    # q = parser.parse('traits.z = traits.y')
    # print(q)


def test_parser_exact_match():
    parser = SqlSearchQueryParser()
    q = parser.parse('traits.other.a == "A"')
    assert q == {'term': {'traits.other.a': {'value': 'A'}}}
    q = parser.parse('traits.other.a is "A"')
    assert q == {'term': {'traits.other.a': {'value': 'A'}}}
    q = parser.parse('traits.other.a is "A*"')
    assert q == {'wildcard': {'traits.other.a': {'value': 'A*'}}}


def test_parser_fulltext_match():
    parser = SqlSearchQueryParser()
    q = parser.parse('traits.other.a ~ "A"')
    assert q == {'match': {'traits.other.a': 'A'}}
    q = parser.parse('traits.other.a match "A"')
    assert q == {'match': {'traits.other.a': 'A'}}


def test_parser_in():
    parser = SqlSearchQueryParser()
    q = parser.parse('traits.other in ["A", "B"]')
    assert q == {'terms': {'traits.other': ['A', 'B']}}
