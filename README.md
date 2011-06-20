# DroneStore

### version control for application data

## Install

    sudo python setup.py install

## License

Dronestore is under the MIT License.

## Hello World

    >>> import dronestore
    >>> from dronestore.model import StringAttribute
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