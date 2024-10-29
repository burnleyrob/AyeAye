from collections import defaultdict
from dataclasses import dataclass
from inspect import isclass

from ayeaye.model import Model


class ModelCollection:
    def __init__(self, models, class_level_eval=False):
        """
        Build graphs from a group of subclasses of :class:`ayeaye.Models`.

        Use the graph of models to
        * find data provenance
        * interactively determine which models have their data dependences satisfied and are
          ready to run.

        @param models (set of subclases of :class:`ayeaye.Models`).
            These aren't instances of the class but the class itself.

        @param class_level_eval: (bool) When True, just use the class declarations to determine the
            relationships between classes. This avoids prepare dataset connectors which often requires
            the connector_resolver. See :class:`ayeaye.connect_resolve.ConnectorResolver` for more.
        """

        invalid_construction_msg = (
            "models must be a class, list, set or callable. All of which result in one or more "
            ":class:`ayeaye.Models` classes (not instances)."
        )

        if not isinstance(models, set):
            raise ValueError(invalid_construction_msg)

        if not all([isclass(m) and issubclass(m, Model) for m in models]):
            raise ValueError(invalid_construction_msg)

        self.models = models
        self.class_level_eval = class_level_eval

        # see :meth:`ready_to_run`
        self._completed_models = set()

        # lazy, see :meth:`model_dependencies`
        self._model_dependencies = None

    @property
    def model_dependencies(self):

        if self._model_dependencies is None:
            self._model_dependencies = self._resolve_model_dependencies()

            # this raises an exception if there is a cycle
            assert self._valid_model_dependencies(self._model_dependencies)

        return self._model_dependencies

    def ready_to_run(self, just_completed=None):
        """
        Note - this method doesn't take into account models that are currently running.

        @param just_completed: (set of model classes) signal that these models have been completed.
            The return to this method will take these into account.

        @return: set of `self.models` that have had all dependencies satisfied.
        """

        if just_completed:
            for model_cls in just_completed:
                if model_cls not in self.models:
                    raise ValueError(f"Model {model_cls.__name__} isn't known to ModelCollection")

                self._completed_models.add(model_cls)

        ready_models = set()
        for model_cls, depends_on in self.model_dependencies:

            if model_cls in self._completed_models:
                # model has already been run
                continue

            if depends_on.issubset(self._completed_models):
                ready_models.add(model_cls)

        return ready_models

    def _base_graph(self):

        if self.class_level_eval:
            raise NotImplementedError("TODO _base_graph_no_resolve")
        else:
            return self._base_graph_with_resolve()

    def _base_graph_with_resolve(self):
        """Find all the datasets in all the models and classify datasets as 'sources' (READ access)
        and 'targets' (WRITE access) or READWRITE for both.

        This version of _base_graph needs the connect_resolve in order to evaluate engine_urls.

        @return: (targets, sources) (dict, dict) - data_flow_key -> set of tuples
                (ModelCls, dataconnector attrib name) for both

            targets - one or more models writes to these
            sources - one or more models read from these

        """
        # data_flow_key -> (ModelCls, dataconnector attrib name)
        targets = defaultdict(set)
        sources = defaultdict(set)

        # ensure resolution to a name string doesn't contain clashes
        check_names = set()

        for model_cls in self.models:
            # TODO find ModelConnectors and recurse into those
            model_name = model_cls.__name__
            if model_name in check_names:
                raise ValueError(f"Duplicate node found: {model_name}")
            check_names.add(model_name)

            # Not instantiating the model `model_obj = model_cls()` because it's not needed
            # in order to find data dependencies.
            for class_attrib_label, connect in model_cls.connects().items():

                # this requires connect_resolve
                data_connector = connect._prepare_connection()

                data_flow = data_connector.data_flow()

                for data_flow_key in data_flow.inputs:
                    sources[data_flow_key].add((model_cls, class_attrib_label))

                for data_flow_key in data_flow.outputs:
                    targets[data_flow_key].add((model_cls, class_attrib_label))

        return targets, sources

    def _resolve_model_dependencies(self):
        """
        Use the dataset connections in each model to determine the dependencies between models and
        therefore the order to run them in.

        @return: list of tuples (Model, {Models dependencies}
                dependencies are those models which must be completed before `Model` can run.

        """
        targets, sources = self._base_graph()

        r = []
        for model_cls in self.models:

            # no attempt at efficiency

            data_deps = set()
            for data_flow_key, deps in sources.items():
                for dep_model_cls, _class_attrib_label in deps:
                    if model_cls == dep_model_cls:
                        data_deps.add(data_flow_key)

            model_deps = set()
            for data_flow_key, target_deps in targets.items():

                if data_flow_key not in data_deps:
                    continue

                for dep_model_cls, _class_attrib_label in target_deps:
                    model_deps.add(dep_model_cls)

            # READWRITE results in a model being dependent on itself so remove that
            model_deps.discard(model_cls)

            # print(model_cls.__name__, data_deps, [m.__name__ for m in model_deps])
            r.append((model_cls, model_deps))

        return r

    def _valid_model_dependencies(self, model_deps):
        """
        raise ValueError if there is any funny cyclic business going on.

        @param model_deps: from :meth:`_resolve_model_dependencies`
        """
        deps_lookup = {model_cls: model_d for model_cls, model_d in model_deps}

        def build_path(deps_lookup, m, ancestors):
            "Find all paths. Not really needed, just the detection of cycles."

            new_nodes = []
            for m2 in deps_lookup.get(m, []):
                if m2 in ancestors:
                    raise ValueError("Cycle detected")
                new_nodes.append(m2)

            if len(new_nodes) == 0:
                yield ancestors + [m]
            else:
                for mx in new_nodes:
                    yield from build_path(deps_lookup, mx, ancestors + [m])

        for model_cls, _model_d in model_deps:

            _all_paths = [p for p in build_path(deps_lookup, model_cls, [])]

            # for p in _all_paths:
            #     debug_path = "-".join([px.__name__ for px in p])
            #     print(debug_path)

        return True

    def as_edges(self):
        """
        Layout model dependencies as graph edges.

        @param model_deps: from :meth:`_resolve_model_dependencies`
        @return: set of :class:`ModelGraphEdge`
        """
        # TODO duplicate call to _base_graph
        # targets, sources = self._base_graph()
        model_deps = self._resolve_model_dependencies()
        #
        # # Model_cls -> dict( sources : list, targets : list)
        # # sources and targets are list of data_flow_key (str)
        # datasets_lookup = {}
        # for label, rels in [("targets", targets), ("sources", sources)]:
        #     for data_flow_key, model_att in rels.items():
        #         for model_cls, _class_attrib_label in model_att:
        #             if model_cls not in datasets_lookup:
        #                 datasets_lookup[model_cls] = defaultdict(list)
        #
        #             datasets_lookup[model_cls][label].append(data_flow_key)

        edge_set = set()
        for model_cls, model_depends_on in model_deps:
            for model_d in model_depends_on:
                edge = ModelGraphEdge(
                    model_a=model_d,
                    model_b=model_cls,
                    dataset_label="",  # data_flow_key here would be ace
                )
                edge_set.add(edge)

        return edge_set


@dataclass
class ModelGraphEdge:
    """
    A representation of the datasets and models as is a list of edges.

    Two :class:`ayeaye.Model`s and a 'dataset' (:class:`Connect` or `DataConnector`) is an edge.
    This was an arbitrary decision, it could have been two datasets and a model.
    """

    model_a: Model
    model_b: Model
    dataset_label: str

    def __hash__(self):
        return hash((self.model_a, self.model_b, self.dataset_label))


class VisualiseModels:
    "Experiment to visualise run order and data provenance for a :class:`ModelCollection` instance"

    def __init__(self, model_collection):
        """
        @param model_collection - instance of :class:`ModelCollection`
        """
        self.model_collection = model_collection

    def mermaid_data_provenance(self):
        """
        @return (str)
            mermaid format (see https://github.com/mermaid-js/mermaid#readme) to visualise model's
            data dependencies.
        """

        def _leaf_label():
            "return name (str) for a leaf node"
            r = 0
            while True:
                yield f"leaf_{r}([ ])"
                r += 1

        # no idea if leaves are the same dataset or not
        leaf_label = _leaf_label()

        edgeset = self.model_collection.as_edges()
        if len(edgeset) == 0:
            return ""

        out = ["graph LR"]
        for edge in edgeset:
            model_a = edge.model_a.__name__ if edge.model_a is not None else next(leaf_label)
            model_b = edge.model_b.__name__ if edge.model_b is not None else next(leaf_label)

            edge_rep = edge.dataset_label or "?"

            edge_fmt = f"{model_a}-->|{edge_rep}| {model_b}"
            out.append(edge_fmt)

        return "\n".join(out)
