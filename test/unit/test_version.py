from tracardi.domain.version import Version


def test_version_name():
    version = Version(version="081")
    assert version.name == Version._generate_name("081")