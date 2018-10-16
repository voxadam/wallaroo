from pickle import dumps, loads

def attach_to_module(func, identifier):
    # Do some scope mangling to create a uniquely named class based on
    # the decorated function's name and place it in the wallaroo module's
    # namespace so that pickle can find it.

    func.__name__ = identifier
    globals()[identifier] = func
    return globals()[identifier]


class Message(object):
    def __init__(self, value, key=None, tags=None, states=None):
        self.value = value
        self.key = key
        self.tags = tags if tags else []
        self.states = states if states else []

    def new_from_key(self, key):
        return Message(self.value, "{}.{}".format(self.key, key),
                       self.tags, self.states)

    def tag(self, tag, state=None, key=None):
        self.tags.append(tag)
        self.states.append(state)

    def encode(self):
        return dumps(self)

    def __str__(self):
        return ("{{key: {key}, value: {value}, tags: {tags}, "
                " states: {states}}}".format(
                    key=self.key,
                    value=self.value,
                    tags=self.tags,
                    states=self.states))

    @staticmethod
    def decode(bs):
        return loads(bs)


class State(object):
    _partitioned = False
    def __init__(self):
        self._a = None
        self._b = None
        self._initialized = False

    def update(self, data):
        if self._initialized:
            if self._partitioned:
                assert(self._b[0] == data.key) # check key compatibility
        else:
            self._initialized = True

        # state update:
        self._a = self._b
        self._b = (data.key, data.value)

    def __str__(self):
        return ("{{_a: {a}, _b: {b}, _initialized: "
                "{initialized}}}".format(
                    a = self._a,
                    b = self._b,
                    initialized = self._initialized))

    def clone(self):
        return (self._a, self._b)


class PartitionedState(State):
    _partitioned = True


def partition(msg):
    return msg.key


def Tag(identifier):
    identifier = "tag_{}".format(identifier)
    def tag(data):
        print("{}({})".format(identifier, data))
        data.tag(identifier)
        return data
    return attach_to_module(tag, identifier)


def TagState(identifier):
    identifier = "tagstate_{}".format(identifier)
    def tagstate(data, state):
        print("{}({}, {})".format(identifier, data, state))
        state.update(data)
        data.tag(identifier, state.clone())
        return(data, True)
    return attach_to_module(tagstate, identifier)


def TagToMany(identifier, num):
    identifier = "tagtomany_{}".format(identifier)
    def tagtomany(data):
        print("{}({})".format(identifier, data))
        data.tag(identifier)
        return [data.new_from_key(x) for x in range(num)]
    return attach_to_module(tagtomany, identifier)


def test_components():
    m = Message(1,1)
    s = State()
    t1 = Tag(1)
    ts1 = TagState(1)
    t1(m)
    ts1(m,s)
    assert(m.value == 1)
    assert(m.key == 1)
    assert(m.tags == ['tag_1', 'tagstate_1'])
    assert(m.states == [None, (None, (1, 1))])


def test_serialisation():
    import wallaroo

    m1 = Message(1,1)
    m2 = Message(1,1)
    s1 = State()
    s2 = State()
    t1 = Tag(1)
    ts1 = TagState(1)
    wt1 = wallaroo.computation("tag1")(t1)
    wts1 = wallaroo.state_computation("tagstate1")(ts1)
    wt1.compute(m1)
    wts1.compute(m1, s1)
    assert(m1.value == 1)
    assert(m1.key == 1)
    assert(m1.tags == ['tag_1', 'tagstate_1'])
    assert(m1.states == [None, (None, (1, 1))])

    # serialise, deserialse, then run again
    ds_wt1 = loads(dumps(wt1))
    ds_wts1 = loads(dumps(wts1))
    ds_wt1.compute(m2)
    ds_wts1.compute(m2, s2)

    assert(m2.value == 1)
    assert(m2.key == 1)
    assert(m2.tags == ['tag_1', 'tagstate_1'])
    assert(m2.states == [None, (None, (1, 1))])


def test_state():
    s = State()
    sp = PartitionedState()
    m1 = Message(1,1)
    m2 = Message(2,2)
    s.update(m1)
    s.update(m2)
    sp.update(m1)
    sp.update(m1)
    try:
        sp.update(m2)
    except Exception as err:
        assert(isinstance(err, AssertionError))
