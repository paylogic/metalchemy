"""Metalchemy tests."""
import pytest
import mock

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Integer
from sqlalchemy.schema import Column

import metalchemy

Base = declarative_base()


@pytest.yield_fixture
def session(engine):
    """SQLAlchemy session."""
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


@pytest.fixture(scope='session')
def engine(request, model_class):
    """SQLAlchemy engine."""
    engine = create_engine('sqlite:///:memory:')
    model_class.metadata.create_all(engine)
    return engine


@pytest.fixture(scope='session')
def metalchemy_attributes():
    """Metalchemy attributes."""
    return metalchemy.initialize(Base)


@pytest.fixture(scope='session')
def model_class(metalchemy_attributes):
    """Test model class."""
    class TestModel(Base):

        """Test model using metalchemy."""

        __tablename__ = 'test'
        __table_args__ = {'extend_existing': True}
        id = Column(Integer, primary_key=True)
        meta = metalchemy_attributes.Metadata()

    return TestModel


@pytest.fixture
def model(model_class):
    """Create TestModel instance for metadata testing."""
    return model_class()


@pytest.mark.parametrize('value', ['some value', True, 10])
def test_attr_storage(session, model, model_class, metalchemy_attributes, value):
    """Test simple case when meta attribute should be stored."""
    session.add(model)
    session.commit()

    model.meta.attr = value

    model = session.query(model_class).get(model.id)
    assert isinstance(model.meta.attr, metalchemy_attributes.FieldWrapper)
    assert model.meta.attr.get_value() == value


def test_FieldWrapper__init_values(model, metalchemy_attributes):
    """Test FieldWrapper's _init_values.

    Here we emulate rare case when meta field doesn't have wrapper, but also doesn't have a wrapped parent
    """
    field = model.meta.attr.sub_attr

    parent_field = field._FieldWrapper__parent
    parent_field._FieldWrapper__parent = None

    field._FieldWrapper__wrapped = None

    with mock.patch.object(metalchemy_attributes.FieldWrapper, '_init_values') as mock_method:
        field._init_values()

        mock_method.assert_called_twice_with()


def test_fieldWrapper_nonzero(model):

    field = model.meta.attr.sub_attr
    assert not bool(field)

    field = 'string'
    assert bool(field)
