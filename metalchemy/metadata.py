"""This module contains the SQLAlchemy based key/value storage system.

Needs special initialization procedure before usage.
please refer to README.rst of the package for more information.

public functions: initialize.
"""
from collections import namedtuple
import sys

from sqlalchemy import (
    and_,
    inspect,
    Integer,
    String,
)
from sqlalchemy.orm import (
    aliased,
    class_mapper,
    query,
    relation,
)
from sqlalchemy.schema import (
    Column,
    ForeignKey,
    Index,
    UniqueConstraint,
)

from metalchemy.types import JSONEncodedText

_root_path = '[root]'
_index_path = ''


# Tuple class describing the return value of the initialization function
Attributes = namedtuple('Attributes', ['Object', 'Field', 'Value', 'FieldWrapper', 'Metadata'])

# True if we are running on Python 3.
PY3 = sys.version_info[0] == 3


class UnicodeMixin(object):

    """Mixin class to handle defining the proper __str__/__unicode__ methods in Python 2 or 3."""

    if PY3:
        def __str__(self):
            return self.__unicode__()
    else:
        def __str__(self):
            return self.__unicode__().encode('utf8')


def get_object_id(obj):
    """Get object's id (primary key)."""
    if not isinstance(obj, type):
        cls = obj.__class__
    else:
        cls = obj
    return getattr(obj, class_mapper(cls).primary_key[0].name)


def initialize(base, table_name_prefix='metalchemy_'):
    """Initialize using given declarative base.

    :param base: SQLAlchemy declarative base class
    :param table_name_prefix: `str` prefix to use for table names
    :return: `Attributes` object which contains:
        * Object class
        * Field class
        * Value class
        * FieldWrapper class
        * Metadata class
    """
    class Object(base):

        """An alchemy model to assign a unique id to a Python object path.

        This model also initializes the hierarchical structure of the fields.

        Property:
        path (<str>) - A dotted path to the class we store metadata for.
        """

        __tablename__ = table_name_prefix + 'object'

        id = Column(Integer, primary_key=True)
        path = Column(String(length=60), nullable=False, unique=True)

        def __getattribute__(self, name):
            if name == 'fields' and 'fields' not in self.__dict__:
                # Expand the flat list of fields to a hierarchical structure
                nodes = {}
                for node in self._fieldnodes:
                    node.children = {}
                    nodes[node.id] = node

                fields = None
                for node in self._fieldnodes:
                    if node.parent_id is None:
                        fields = node
                    else:
                        nodes[node.parent_id].children[node.name] = node

                self.fields = fields
                return self.fields
            else:
                return super(Object, self).__getattribute__(name)

        def __repr__(self):
            return '<Object for instances of ' + self.path + '>'

    class Field(base):

        """Alchemy model for a single field definition.

        Fields can be nested in other fields.

        Properties:
        object - The <Object> to which this field belongs.
        parent - The parent <Field>, or None if this is the root field for an object.
        name   - The attribute name under which this field is available.
        title  - A string that can be used as a label when displaying this field.

        children - A dictionary of child <Fields>, where the key is the name of that child.
        """

        __tablename__ = table_name_prefix + 'field'

        id = Column(Integer, primary_key=True)
        obj_id = Column(
            Integer, ForeignKey(Object.__table__.c.id), nullable=False)
        name = Column(String(length=60), nullable=False, index=True)
        parent_id = Column(Integer, ForeignKey(id))
        title = Column(String(length=30), nullable=False, default='')

        object = relation(
            Object, backref='_fieldnodes', lazy=False, order_by=parent_id)
        parent = relation('Field', remote_side=id)

        __table_args__ = (UniqueConstraint(
            'obj_id', 'name', 'parent_id'), getattr(base, '__table_args__', {}))

        def __init__(self, *args, **kwargs):
            super(Field, self).__init__(*args, **kwargs)
            self.children = {}

        def __unicode__(self):
            return self.name

    class Value(base):

        """An alchemy model representing a value assigned to a metadata field for a specific object instance.

        Properties:
        instance_id - The primary key for the class instance to which this metadata value belongs.
        field       - The <Field> for which this is a value.
        idx         - The index in the array of values for the field.
        parent      - The parent <Value> (for hierarchical arrays)
        value       - The actual stored value.
        """

        __tablename__ = table_name_prefix + 'values'

        id = Column(Integer, primary_key=True)
        instance_id = Column(Integer, nullable=False, index=True)
        field_id = Column(
            Integer, ForeignKey(Field.__table__.c.id), nullable=False)
        idx = Column(Integer, nullable=False)
        parent_idx = Column(Integer, ForeignKey(id))
        value = Column(JSONEncodedText, nullable=True)

        field = relation(Field)
        parent = relation('Value', remote_side=id)

        def __unicode__(self):
            return u'id={0}, field={1}, idx={2}, parent={3}, value={4}'.format(
                self.id,
                repr(self.field), self.idx, self.parent_idx, self.value
            )
    Index('value', Value.value, mysql_length=8)

    class FieldWrapper(UnicodeMixin):

        """A helper class for a field for a specific object instance."""

        def __init__(self, parent, idxpath, indexed, wrapped, instance):
            """Initialize field wrapper instance.

            :param parent: `FieldWrapper` wrapper containing this wrapper, or None if this is the root.
            :param idxpath: - `str` index path to reach this specific field
            :param indexed: `int` indicates the index for this wrapper instance, or None if it is an unindexed instance
            :param wrapped: `Field` - The field to wrap.
            :param instance: - An instance of the class containing metadata.
            """
            self.__parent = parent
            self.__idxpath = idxpath
            self.__indexed = indexed
            self.__wrapped = wrapped
            self.__instance = instance
            self.__fields = {}

        def _get_name(self):
            """Return the name of this field."""
            if isinstance(self.__wrapped, Field):
                return self.__wrapped.name
            else:
                return self.__wrapped

        def _get_real_parent(self):
            """Return the parent wrapper that is not indexed."""
            if self.__indexed is not None:
                return self.__parent._get_real_parent()
            if self.__parent is not None and self.__parent.__indexed is not None:
                return self.__parent.__parent
            else:
                return self.__parent

        def _path(self):
            """Return a string describing the complete path to this field."""
            if self.__indexed is not None:
                return self.__parent._path()
            path = self._get_name()
            curr = self.__parent
            while curr is not None:
                if curr.__indexed is None:
                    path = (curr._get_name() or '') + '.' + path
                curr = curr.__parent
            return path

        def _init_values(self):
            """Load all metadata values for the wrapped object instance."""
            # Find a field node which has an actual Field instance associated with it, we need that
            # for its *object* property.
            field = self
            while not isinstance(field.__wrapped, Field):
                field = field.__parent
                if field.__parent is None:
                    # rare case, but happens
                    break

            values = {}
            try:
                objlist = (
                    inspect(self.__instance).session.query(Value)
                    .join(Field).filter(and_(
                        Field.obj_id == field.__wrapped.obj_id,
                        Value.instance_id == get_object_id(self.__instance))
                    ).all()
                )
            except Exception:
                objlist = []

            for v in objlist:
                if v.field_id not in values:
                    values[v.field_id] = []
                values[v.field_id].append(v)

            indexes = {}

            def set_values(path, field):
                if field.id in values:
                    for val in values[field.id]:
                        if val.parent is None:
                            idxpath = ''
                        else:
                            idxpath = indexes[val.parent]
                        idxpath += '.' + str(val.idx)
                        indexes[val] = idxpath
                        if path not in self.__instance._meta_values:
                            self.__instance._meta_values[path] = {}
                        self.__instance._meta_values[path][idxpath] = val

                for name, child in field.children.items():
                    set_values(path + '.' + name, child)

            self.__instance._meta_values = {_root_path: {}}
            set_values(
                field.__wrapped.object.fields.name, field.__wrapped.object.fields)
            self.__instance._meta_inited = True

        def _get_value(self):
            """Return the <Value> wrapped by this particular FieldWrapper."""
            path = self._path()
            idxpath = self.__idxpath
            if self.__indexed is not None:
                idxpath += '.' + str(self.__indexed)
            else:
                idxpath += '.0'
            if idxpath not in self.__instance._meta_values.get(path, ()):
                return None
            return self.__instance._meta_values[path][idxpath]

        def get_value(self):
            """Return the field's contained value."""
            if not self.__instance._meta_inited:
                self._init_values()

            value = self._get_value()

            return value.value if value is not None else None

        def __contains__(self, item):
            return item in self.get_value()

        def __getitem__(self, key):
            """Retrieve a `FieldWrapper` for a specific index of the current wrapper."""
            if not isinstance(key, int):
                raise TypeError(
                    'list indices must be integers, not ' + key.__class__.__name__)

            if self.__indexed is None:
                return FieldWrapper(self, self.__idxpath, key, self.__wrapped, self.__instance)
            else:
                return getattr(self, _index_path)[key]

        def __getattr__(self, name):
            """Handle getting attribute values of a field - i.e., retrieving child fields.

            This is equivalent to requesting the named attribute from the zeroth index: field.attr == field[0].attr
            """
            if name[:1] == '_':
                return self.__dict__[name]

            elif self.__indexed is None:
                return getattr(self[0], name)

            else:
                if name not in self.__fields:
                    # The requested attribute has not yet been wrapped
                    if isinstance(self.__wrapped, Field) and name in self.__wrapped.children:
                        # The requested attribute already exists in the hierarchy
                        # as a Field
                        self.__fields[name] = FieldWrapper(self, self.__idxpath + '.' + str(self.__indexed), None,
                                                           self.__wrapped.children[name], self.__instance)
                    else:
                        # The requested attribute does not yet exist in the
                        # hierarchy
                        self.__fields[name] = FieldWrapper(self, self.__idxpath + '.' + str(self.__indexed), None,
                                                           name, self.__instance)

                return self.__fields[name]

        def __bool__(self):
            return bool(self.get_value())

        def __nonzero__(self):
            return self.__bool__()

        def __setitem__(self, key, value):
            """Handle writing to an indexed value."""
            if self.__indexed is not None:
                # Writing to an already-indexed field requires getting the implicit
                # child for sub-indexing
                getattr(self, _index_path)[key] = value
                return

            if not isinstance(key, int):
                raise TypeError(
                    'list indices must be integers, not ' + key.__class__.__name__)

            if not self.__instance._meta_inited:
                self._init_values()

            self._ensure_writable()

            val = self[key]._get_value()
            session = inspect(self.__instance).session
            if val:
                val.value = value
            else:
                # There is no Value for the attribute and index yet, so create
                # one.
                assert get_object_id(self.__instance) is not None, (
                    'Metadata cannot be associated with an object '
                    'that does not yet have an id')
                newvalue = Value(
                    instance_id=get_object_id(self.__instance),
                    field=self.__wrapped,
                    idx=key,
                    parent=self.__parent._get_value(
                    ) if self.__parent is not None else None,
                    value=value
                )
                if self._path() not in self.__instance._meta_values:
                    self.__instance._meta_values[self._path()] = {}
                self.__instance._meta_values[
                    self._path()][self.__idxpath + '.' + str(key)] = newvalue
                val = newvalue
                session.add(val)
            session.flush()

        def __setattr__(self, name, value):
            """setattr handles setting of attribute values on a field - i.e. assigning values to child fields."""
            if name[:1] == '_':
                super(FieldWrapper, self).__setattr__(name, value)
            elif self.__indexed is None:
                setattr(self[0], name, value)
            else:
                # Assert there are no dots in the name, as this can potentially
                # lead to some horrible problems.
                assert name.find(
                    '.') == -1, 'Cannot assign to names with a dot in them'
                getattr(self, name)[0] = value

        def __repr__(self):
            if isinstance(self.__wrapped, Field):
                if not self.__instance._meta_inited:
                    value = 'unknown value'
                else:
                    value = repr(self.get_value())
            else:
                value = '\'\''
            return '<Metadata field {0}[{1}]{2}: {3}>'.format(
                self._path(),
                self.__idxpath,
                '[' + str(self.__indexed) +
                ']' if self.__indexed is not None else '',
                value
            )

        def _force_hierarchy(self):
            """Ensure that the path in the <Field> structure to the contained field exists.

            Create any missing fields.
            """
            if isinstance(self.__wrapped, Field):
                return

            if self.__indexed is not None:
                self.__parent._force_hierarchy()
                # Indexed wrappers wrap the exact same instance as the non-indexed
                # wrapper
                self.__wrapped = self.__parent.__wrapped
            else:
                # We need to make a Field in the hierarchy.
                # If parent is None, we are the root node, and the root node should always be created whenever
                # an Object is created. So in that case, there is no need to
                # create anything.
                if self.__parent is not None:
                    self.__parent._ensure_writable()
                    name = self.__wrapped
                    self.__wrapped = Field(
                        name=name, object=self.__parent.__wrapped.object, parent=self.__parent.__wrapped)
                    self.__parent.__wrapped.children[name] = self.__wrapped
                    session = inspect(self.__wrapped).session
                    session.add(self.__wrapped)
                    session.flush()

        def _force_values(self):
            """Ensure that for each point in the path to the wrapped value a value exists."""
            if self.__parent is not None:
                self.__parent._force_values()

            if self.__indexed is not None:
                path = self._path()
                idxpath = self.__idxpath + '.' + str(self.__indexed)

                if (path not in self.__instance._meta_values or
                        idxpath not in self.__instance._meta_values[path]):
                    self.__parent[self.__indexed] = ''

        def _ensure_writable(self):
            """Ensure that the `Field` path exists (see `_force_hierarchy`).

            Then ensure values exists for the entire path (`_force_values`).
            """
            self._force_hierarchy()
            if self.__parent is not None:
                self.__parent._force_values()

        def append(self, value):
            """Append the given value to the list."""
            if self.__indexed is not None:
                getattr(self, _index_path).append(value)
            else:
                self._init_values()
                idx = None
                if self._path() in self.__instance._meta_values:
                    for idxkey in self.__instance._meta_values[self._path()]:
                        idx = max(idx, int(idxkey.rsplit('.', 1)[-1]))
                if idx is None:
                    self[0] = value
                else:
                    self[idx + 1] = value

        # Iteration-related methods
        def _iteritems(self):
            """The actual generator used for <iteritems>. Only valid on nonindexed wrappers."""
            if not self.__instance._meta_inited:
                self._init_values()
            try:
                for idxkey, value in self.__instance._meta_values[self._path()].items():
                    yield int(idxkey.rsplit('.', 1)[-1]), value.value
            except KeyError:
                pass

        def iteritems(self):
            """Iterate over both the keys and values of the list.

            Note that there is no guarantee of ordering!
            """
            if self.__indexed is not None:
                return getattr(self, _index_path).items()
            else:
                return self._iteritems()

        def _iter(self):
            if not self.__instance._meta_inited:
                self._init_values()
            for item in self.__instance._meta_values[self._path()].values():
                yield item.value

        def __iter__(self):
            """Iterator over the list's values.

            Note that there is no guarantee of ordering!
            """
            if self.__indexed is not None:
                return getattr(self, _index_path).__iter__()
            else:
                return self._iter()

        def _iterchildren(self):
            if not self.__instance._meta_inited:
                self._init_values()
            if isinstance(self.__wrapped, Field):
                for name in self.__wrapped.children.iterkeys():
                    yield getattr(self, name)

        def iterchildren(self):
            """Iterate over the child fields of this field.

            Yield (fieldname, `FieldWrapper`) tuples.
            """
            if self.__indexed is None:
                return self[0].iterchildren()
            else:
                return self._iterchildren()

    class Metadata(object):

        """A descriptor class providing access to the metadata system.

        Any object whishes to have a metadata property must add an attribute with an instance of this class.

        :see: http://docs.python.org/reference/datamodel.html#implementing-descriptors
        """

        def __init__(self):
            self.fields = None

        def __get__(self, instance, owner):
            if instance is None:
                return self

            if self.fields is None:
                session = inspect(instance).session
                path = owner.__module__ + '.' + owner.__name__
                self.fields = session.query(Object).filter_by(path=path).first()
                if not self.fields:
                    self.fields = session.query(Object).filter_by(path=path).first()
                if self.fields is None:
                    # No meta yet
                    self.fields = Object(path=path)
                    Field(object=self.fields, name=_root_path)
                    session.add(self.fields)
                    session.flush([self.fields])

            if not hasattr(instance, '_meta_fields'):
                instance._meta_fields = FieldWrapper(
                    None, '', None, self.fields.fields, instance)
                instance._meta_values = {}
                instance._meta_inited = not get_object_id(instance)

            return instance._meta_fields

        def __set__(self, instance, owner):
            raise AttributeError('Cannot write directly to a metadata instance')

        def __delete__(self, instance):
            raise AttributeError('Cannot delete a metadata instance')

        def filter(self, owner, field, value):
            """Search the metadata. It returns a query on the owner class.

            :param owner: - The owner class model (e.g Order)
            :param field: - The field to search on (e.g sparq.ticketware.user_id)
            :param value: - The value to search for
            :param operator: - The operator to apply to the field and value. Optional; defaults to equality.
            """
            if not field.startswith('[root]'):
                field = '.'.join(['[root]', field])
            value_alias = aliased(Value)
            session = inspect(owner).session
            query = (
                session.query(owner.__class__)
                .join((value_alias, value_alias.instance_id == get_object_id(owner)))
                .join((Object, Object.path == format('.'.join([owner.__module__, owner.__name__]))))
            )
            query = query.filter(Value.value == value)
            parents = field.split('.')

            prev = None
            for parent_name in parents:
                f = aliased(Field)
                cond = f.obj_id == Object.id
                if prev is not None:
                    cond = and_(cond, f.parent_id == prev.id)
                query = query.join((f, cond)).filter(f.name == parent_name)
                prev = f
            query = query.filter(value_alias.field_id == f.id)

            return query

    class Query(query.Query):

        """Provides a filter_by_meta command for sqlalchemy's query.

        Extend your query class from this, together with sqlalchemy.orm.Query.
        """

        def filter_by_meta(self, field, value):
            """Search the metadata.

            It will be always searched from the root (you don't need to add [root]).

            :param field: - The field to search on (e.g sparq.ticketware.user_id)
            :param value: - The value to search for. It will be converted to string.
            """
            from paylogic.core.metadata import Object, Field, Value

            if not field.startswith('[root]'):
                field = '[root].' + field

            value = str(value)
            owner = self._mapper_adapter_map.keys()[0]
            query = (
                self
                .join((Value, Value.instance_id == owner.id))
                .join((Object, Object.path == '.'.join([owner.__module__, owner.__name__])))
            )
            query = query.filter(Value.value == value)

            prev = None
            for name in field.split('.'):
                f = aliased(Field)
                cond = f.obj_id == Object.id
                if prev is not None:
                    cond = and_(cond, f.parent_id == prev.id)
                query = query.join((f, cond)).filter(f.name == name)
                prev = f

            query = query.filter(Value.field_id == f.id)

            return query

        def meta_outerjoin(self, cls, name, value_cls=None):
            """Perform an outerjoin with the metadata system.

            The join is done in such a way that if the object has no metadata value, it's not dropped from the result.

            :param cls: - The class whose metadata is to be joined in.
            :param name: - The name of the metadata property to join in (may be in dotted notation (foo.bar.baz)).
            :param value_cls: - Optional; the class of the metadata value. When omitted,
                this defaults to paylogic.core.metadata.Value. The
                parameter is provided, because this class must be aliased if
                you want multiple metavalues in the join. (See the second
                example below.)

            :returns: A `Query` object with the join applied.

            Examples:
            If you want all Foo objects with the value of their 'a.b.c' metadata
            property, use the following code:

              session.query(Value).meta_outerjoin(Foo, 'a.b.c').all()

            This will return a list of tuples of Foo and Value objects. If a Foo
            instance has no 'a.b.c' metadata property, the second element of the
            tuple will be None.

            If you want to join in multiple metadata properties, they need to be
            aliased:

              from sqlalchemy.orm import aliased
              V1 = aliased(Value)
              V2 = aliased(Value)
              session.query(V1, V2).meta_outerjoin(Foo, 'a.b.c', V1).meta_outerjoin(Foo, 'c.b.a', V2).all()

            This will return a list of tuples consisting of a Foo instance and two
            Value instances. Missing metadata values are None values in the tuples.
            """
            # Get the Object corresponding to the class whose metadata is to be retrieved
            obj = self.session.query(Object).filter(Object.path == cls.get_path()).first()

            # Get the Field corresponding to the specified name
            parts = name.split('.')

            # Get the root field
            field = self.session.query(Field).filter(and_(Field.object == obj, Field.parent.is_(None))).first()
            for part in parts:
                # Iterate through the path to find the correct field
                field = self.session.query(Field).filter(and_(Field.name == part, Field.parent == field)).first()

            # Get the value class
            if not value_cls:
                value_cls = Value

            # Append the join to the current query and return
            return self.outerjoin((value_cls, and_(value_cls.field == field, value_cls.instance_id == cls.id)))

    query.Query = Query

    return Attributes(Object=Object, Field=Field, Value=Value, FieldWrapper=FieldWrapper, Metadata=Metadata)
