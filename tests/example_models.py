import ayeaye
from ayeaye.common_pattern.parallel_model_runner import AbstractDependencyDrivenModelRunner


class SimpleModel(ayeaye.Model):
    def build(self):
        pass


class One(SimpleModel):
    a = ayeaye.Connect(engine_url="csv://a")
    b = ayeaye.Connect(engine_url="csv://b", access=ayeaye.AccessMode.WRITE)


class Two(SimpleModel):
    b = One.b.clone(access=ayeaye.AccessMode.READ)
    c = ayeaye.Connect(engine_url="csv://c", access=ayeaye.AccessMode.WRITE)


class Three(SimpleModel):
    c = Two.c.clone(access=ayeaye.AccessMode.READ)
    d = ayeaye.Connect(engine_url="csv://d", access=ayeaye.AccessMode.WRITE)


class Four(SimpleModel):
    b_copy_paste = ayeaye.Connect(engine_url="csv://b", access=ayeaye.AccessMode.READ)
    e = ayeaye.Connect(engine_url="csv://e", access=ayeaye.AccessMode.WRITE)


class Five(SimpleModel):
    b = One.b.clone(access=ayeaye.AccessMode.READ)
    f = ayeaye.Connect(engine_url="sqlite:////data/f.db", access=ayeaye.AccessMode.READWRITE)


class Six(SimpleModel):
    b = One.b.clone(access=ayeaye.AccessMode.READ)
    f = Five.f.clone(access=ayeaye.AccessMode.WRITE)


failed_callable_msg = "No test should be calling this as the parent model class isn't instantiated"


def find_destination():
    """Will be called at build() time. In real life this would find out something that should
    only be looked up during runtime."""
    # return "csv://g.csv"
    raise Exception(failed_callable_msg)


def another_find_destination():
    raise Exception(failed_callable_msg)


class Seven(SimpleModel):
    b = One.b.clone(access=ayeaye.AccessMode.READ)
    g = ayeaye.Connect(engine_url=find_destination, access=ayeaye.AccessMode.WRITE)


class Eight(SimpleModel):
    g = Seven.g.clone(access=ayeaye.AccessMode.READ)
    h = ayeaye.Connect(engine_url="csv://h", access=ayeaye.AccessMode.WRITE)


class Nine(SimpleModel):
    i = ayeaye.Connect(engine_url=another_find_destination, access=ayeaye.AccessMode.WRITE)
    h = ayeaye.Connect(engine_url="csv://h", access=ayeaye.AccessMode.WRITE)


class X(SimpleModel):
    r = ayeaye.Connect(engine_url="csv://r")
    s = ayeaye.Connect(engine_url="csv://s", access=ayeaye.AccessMode.WRITE)


class Y(SimpleModel):
    s = X.s.clone(access=ayeaye.AccessMode.READ)
    t = ayeaye.Connect(engine_url="csv://t", access=ayeaye.AccessMode.WRITE)


class Z(SimpleModel):
    t = Y.t.clone(access=ayeaye.AccessMode.READ)
    u = ayeaye.Connect(engine_url="csv://u", access=ayeaye.AccessMode.WRITE)


class DependenciesModel(AbstractDependencyDrivenModelRunner):
    models = {One, Two, Three, Four, Five, Six}
