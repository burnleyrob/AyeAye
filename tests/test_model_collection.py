import unittest

import ayeaye
from ayeaye.model_collection import ModelCollection, ModelGraphEdge, VisualiseModels


class One(ayeaye.Model):
    a = ayeaye.Connect(engine_url="csv://a")
    b = ayeaye.Connect(engine_url="csv://b", access=ayeaye.AccessMode.WRITE)


class Two(ayeaye.Model):
    b = One.b.clone(access=ayeaye.AccessMode.READ)
    c = ayeaye.Connect(engine_url="csv://c", access=ayeaye.AccessMode.WRITE)


class Three(ayeaye.Model):
    c = Two.c.clone(access=ayeaye.AccessMode.READ)
    d = ayeaye.Connect(engine_url="csv://d", access=ayeaye.AccessMode.WRITE)


class Four(ayeaye.Model):
    b_copy_paste = ayeaye.Connect(engine_url="csv://b", access=ayeaye.AccessMode.READ)
    e = ayeaye.Connect(engine_url="csv://e", access=ayeaye.AccessMode.WRITE)


class Five(ayeaye.Model):
    b = One.b.clone(access=ayeaye.AccessMode.READ)
    f = ayeaye.Connect(engine_url="sqlite:////data/f.db", access=ayeaye.AccessMode.READWRITE)


class Six(ayeaye.Model):
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


class Seven(ayeaye.Model):
    b = One.b.clone(access=ayeaye.AccessMode.READ)
    g = ayeaye.Connect(engine_url=find_destination, access=ayeaye.AccessMode.WRITE)


class Eight(ayeaye.Model):
    g = Seven.g.clone(access=ayeaye.AccessMode.READ)
    h = ayeaye.Connect(engine_url="csv://h", access=ayeaye.AccessMode.WRITE)


class Nine(ayeaye.Model):
    i = ayeaye.Connect(engine_url=another_find_destination, access=ayeaye.AccessMode.WRITE)
    h = ayeaye.Connect(engine_url="csv://h", access=ayeaye.AccessMode.WRITE)


class X(ayeaye.Model):
    r = ayeaye.Connect(engine_url="csv://r")
    s = ayeaye.Connect(engine_url="csv://s", access=ayeaye.AccessMode.WRITE)


class Y(ayeaye.Model):
    s = X.s.clone(access=ayeaye.AccessMode.READ)
    t = ayeaye.Connect(engine_url="csv://t", access=ayeaye.AccessMode.WRITE)


class Z(ayeaye.Model):
    t = Y.t.clone(access=ayeaye.AccessMode.READ)
    u = ayeaye.Connect(engine_url="csv://u", access=ayeaye.AccessMode.WRITE)


class TestModelCollection(unittest.TestCase):
    @staticmethod
    def repr_run_order(run_order):
        """
        @return: (list of sets) showing a simplified representation of the run_order from
        :meth:`_resolve_run_order` using just the 'model_name' field.
        """
        r = []
        for task_group in run_order:
            assert isinstance(task_group, set)
            name_set = set([t.model_name for t in task_group])
            r.append(name_set)
        return r

    # def test_resolve_with_callable(self):
    #     "Seven has a callable to build it's engine_url at build time"
    #     c = ModelCollection(models={One, Eight, Seven})
    #     r = c._resolve_run_order()
    #     self.assertEqual([{"One"}, {"Seven"}, {"Eight"}], self.repr_run_order(r.run_order))
    #
    # def test_resolve_with_two_different_callables(self):
    #     c = ModelCollection(models={One, Nine, Seven})
    #     r = c._resolve_run_order()
    #     self.assertEqual([{"One", "Nine"}, {"Seven"}], self.repr_run_order(r.run_order))
    #

    def test_mermaid_run_order(self):
        "Visualisation experiment - incomplete"

        c = ModelCollection(models={One, Two, Three})
        visual = VisualiseModels(model_collection=c)
        mermaid_content = visual.mermaid_data_provenance()

        self.assertIn("One-->|| Two", mermaid_content)

    def test_model_dependencies(self):

        c = ModelCollection(models={One, Two, Three, Four, Five, Six})
        model_dependencies = c._resolve_model_dependencies()

        readable = []
        for model_cls, model_deps in model_dependencies:
            run_first = {m.__name__ for m in model_deps}
            t = (model_cls.__name__, run_first)
            readable.append(t)

        # deterministic
        readable.sort()

        expected = [
            ("Five", {"Six", "One"}),
            ("Four", {"One"}),
            ("One", set()),
            ("Six", {"One"}),
            ("Three", {"Two"}),
            ("Two", {"One"}),
        ]
        self.assertEqual(expected, readable)

    def test_valid_model_dependencies(self):

        c = ModelCollection(models={One, Four, Five, Six})

        # happy path
        model_deps = [(Five, {Six, One}), (Four, {One})]
        self.assertTrue(c._valid_model_dependencies(model_deps))

        # cyclic
        model_deps = [(One, {Two}), (Two, {Three}), (Three, {One, Four})]
        with self.assertRaises(ValueError):
            c._valid_model_dependencies(model_deps)

    def test_ready_to_run_simple(self):

        c = ModelCollection(models={One, Two, Three, Four, Five, Six})

        leaf_models = c.ready_to_run()
        self.assertEqual({One}, leaf_models)

        for complete, expected_ready in [
            ({One}, {Two, Four, Six}),
            ({Six}, {Two, Four, Five}),
            ({Two, Four}, {Three, Five}),
            ({Three, Five}, set()),
        ]:
            ready = c.ready_to_run(just_completed=complete)
            self.assertEqual(expected_ready, ready)

    def test_ready_to_run_independent_graphs(self):
        "Two independent graphs"

        c = ModelCollection(models={One, Two, Three, X, Y, Z})

        leaf_models = c.ready_to_run()
        self.assertEqual({One, X}, leaf_models)

        next_models = c.ready_to_run(just_completed={X, One})
        self.assertEqual({Two, Y}, next_models)

    def test_as_edges(self):

        c = ModelCollection(models={One, Two, Three, Four, Five, Six})
        edges = c.as_edges()

        expected = {
            ModelGraphEdge(model_b=Five, model_a=Six, dataset_label=""),
            ModelGraphEdge(model_b=Two, model_a=One, dataset_label=""),
            ModelGraphEdge(model_b=Five, model_a=One, dataset_label=""),
            ModelGraphEdge(model_b=Three, model_a=Two, dataset_label=""),
            ModelGraphEdge(model_b=Six, model_a=One, dataset_label=""),
            ModelGraphEdge(model_b=Four, model_a=One, dataset_label=""),
        }

        self.assertEqual(expected, edges)
