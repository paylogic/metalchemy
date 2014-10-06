metalchemy: SQLAlchemy hierarchical key/value helper
====================================================

The ``metalchemy`` package provides helpers for your SQLAlchemy models to add dynamic properties.

.. image:: https://api.travis-ci.org/paylogic/metalchemy.png
   :target: https://travis-ci.org/paylogic/metalchemy
.. image:: https://pypip.in/v/metalchemy/badge.png
   :target: https://crate.io/packages/metalchemy/
.. image:: https://coveralls.io/repos/paylogic/metalchemy/badge.png?branch=master
   :target: https://coveralls.io/r/paylogic/metalchemy


Installation
------------

.. code-block:: sh

    pip install metalchemy


Usage
-----

metalchemy usage example:

.. code-block:: python

    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    import metalchemy

    metalchemy_attributes = metalchemy.initialize(Base)

    Session = sessionmaker(bind=engine)
    sess = Session()

    class MyModel(Base)

        meta = metalchemy_attributes.Metadata()


    my_object = MyModel()
    my_object.meta.some.value = 'some value'
    sess.add(my_object)
    sess.commit()

    sess.refresh(my_object)
    assert  my_object.meta.some.value.get_value() == 'some value'


In order to give a class metadata capabilities, add a single class attribute to it which is an instance
of <Metadata>:

.. code-block:: python

    class HasMetadata(object):
        meta = metadata.Metadata()

Any instance of this class will now have its metadata accessible via the *meta* attribute.

Such meta attributes allow free reading and assigning of attributes, with no limits on the depth of the attributes.
i.e., meta.foo is always available for reading and writing, but also meta.foo.bar.baz, without any setup beforehand.

Assigning to a metadata property is simple and
obvious:

.. code-block:: python

    inst.meta.foo = 42
    inst.meta.foo.bar.baz = 'qux'

Any metadata attribute is also implicitly an array. It is possible to assign and read from any
index:

.. code-block:: python

    inst.meta.foo[0] = 42
    inst.meta.foo[1] = 'baz'
    inst.meta.foo[1].bar = 'qux'
    inst.meta.foo[1].spam[2] = 'eggs'
    inst.meta.foo[1][2] = 'xyzzy'

Any non-indexed attribute is implicitly converted to an index of zero.

All metadata values are converted to unicode strings on assignment. Assigned values are automatically added to
the SQLAlchemy session, but not committed, so remember to execute session.commit().
Furthermore, the methods `FieldWrapper.append`, `FieldWrapper.iteritems` and `FieldWrapper.__iter__`
are supported as well for direct iteration.

Reading the value back requires using `get_value` method:

.. code-block:: python

    inst.meta.foo.get_value()  # returns 42
    inst.meta.foo.bar.baz.get_value()  # returns u'qux'

Internals:
The hierarchical structure of the fields is stored in an adjacency list (represented by <_Fields>), which is unique
for a single class. All instances of a class share this same tree. An <_Object> maps the class name to this tree,
and is set up to have the entire tree load at once when it is needed.

The <Metadata> instance assigned to a container class will load the <_Object> (and implicitly, the field hierarchy)
on access and return a wrapped root node.

<FieldWrappers> wrap each <_Field>, performing two functions:
- They allow accessing fields that have no concrete <_Field> instance yet, creating these as necessary
- They actually access the values list of a specific container class instance.

Values are stored in a flat list, which is loaded entirely for the container class instane when it is first needed.

Array support is handled by having two states of `FieldWrappers`: regular and indexed.

A regular wrapper wraps an unindexed attribute. It handles reading and writing of array elements on its attribute
(`FieldWrapper.__getitem__` and `FieldWrapper.__setitem__`), and defers attribute access to its own zeroth index.

An indexed wrapper wraps an indexed attribute. This wrapper handles attribute access by returning a wrapper for that
child attribute. Indexed wrappers can also be indexed, but this is internally done by deferring the secondary index
to a hidden child attribute. i.e. accessing `meta.foo[0][1][2]` is internally handled as accessing
`meta.foo[0].<hidden>[1].<hidden>[2]`.

.. warning::

    * Before metadata is assigned, primary key must have been set to its value. This means they must be at least flushed once before assigning metadata.


Contact
-------

If you have questions, bug reports, suggestions, etc. please create an issue on
the `GitHub project page <http://github.com/paylogic/metalchemy>`_.


License
-------

This software is licensed under the `MIT license <http://en.wikipedia.org/wiki/MIT_License>`_

See `License <https://github.com/paylogic/metalchemy/blob/master/LICENSE.txt>`_


© 2014 Paylogic International.
