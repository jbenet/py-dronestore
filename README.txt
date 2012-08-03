# DroneStore

### distributed version control for application data

Dronestore is a library that keeps objects and their attributes versioned to
allow merging with different versions of the object at a later date.
Upon merging two object versions, attribute values are selected according to
given rules (e.g. most recent, maximum). Thus, multiple disconnected machines
can modify the same object and sync changes at a later date.

(slides from a talk [here](http://static.juanbb.com/acm.dronestore.pdf))

## Install

    sudo python setup.py install

## License

Dronestore is under the MIT License.

## Hello World

    >>> import dronestore
    >>> from dronestore import StringAttribute
    >>> from dronestore.merge import LatestStrategy
    >>>
    >>> class MyModel(dronestore.Model):
    ...   first = StringAttribute(strategy=LatestStrategy)
    ...   second = StringAttribute(strategy=LatestStrategy)
    ...
    >>> foo = MyModel('FooBar')
    >>> foo.first = 'Hello'
    >>> foo.commit()
    >>>
    >>> bar = MyModel('FooBar')
    >>> bar.second = 'World'
    >>> bar.commit()
    >>>
    >>> foo.merge(bar)
    >>> print foo.first, foo.second
    Hello World