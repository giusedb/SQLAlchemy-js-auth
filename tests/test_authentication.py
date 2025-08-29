import pytest
from sqlalchemy.orm import Session


def test_login():
    pass


def test_authorization(filesystem, session: Session):
    Dir, File = filesystem

    foo = session.get(Dir, 1)
    assert foo.name == 'foo'

    assert len(foo.files) == 1
    assert foo.files[0].name == 'foo'

