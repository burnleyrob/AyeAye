import copy
from multiprocessing import Condition

import ayeaye
from ayeaye.runtime.task_message import TaskComplete, TaskFailed, TaskLogMessage, TaskPartition


class AbstractModelRunner(ayeaye.PartitionedModel):
    """
    Use a `PartitionedModel` to run some normal :class:`ayeaye.Model`s.

    This is overly simplistic, see the Fossa project (https://github.com/Aye-Aye-Dev/fossa) for how
    is should be done in more complex scenarios.

    This class must be subclassed and given a list of models to run. The models mustn't be
    PartitionedModels (as this could create a resource log jam) and the models mustn't depend on each
    other as this isn't checked.

    The models in the list are started in order but they are run in parallel (with the number of
    parallel tasks depending on the number of CPUs) so any dependencies might not have been satisfied.

    @see :meth:`tests.test_model_partitioned.TestPartitionedModel.test_parallel_models` for an
    example of this running.
    """

    # subclass must implement this. It's a list of tuples (model_cls, kwargs) where kwargs is a
    # dictionary.
    models = None

    def partition_plea(self):
        m_count = len(self.models)
        p = ayeaye.PartitionedModel.PartitionOption(minimum=1, maximum=m_count, optimal=m_count)
        return p

    def partition_slice(self, slice_count):
        target_method = "run_etl_model"  # this is the method subtasks will run
        subtasks = [(target_method, {"model_position": p}) for p in range(len(self.models))]
        return subtasks

    def run_etl_model(self, model_position):
        """
        Run one of the target models in a separate process. Re-direct it's log messages to
        `self.log`.

        @param model_position: (int)
            `self.models` is a list, run the model in this position.
        """

        class LoopBackLogger:
            """
            Redirect log messages from the target model to ModelRunner's log method.
            """

            def __init__(self, log_prefix, log_target):
                self.log_prefix = log_prefix
                self.log_target = log_target

            def write(self, msg):
                self.log_target.log(f"{self.log_prefix} {msg}")

        model_cls, model_kwargs = self.models[model_position]
        model_name = model_cls.__name__

        external_log = LoopBackLogger(log_prefix=model_name, log_target=self)

        self.log(f"Running model {model_name} from position: {model_position}")
        m = model_cls(**model_kwargs)

        m.set_logger(external_log)
        m.log_to_stdout = False  # avoid duplicate messages

        m.go()

    def build(self):
        self.log("Running ModelRunner")


class AbstractDependencyDrivenModelRunner(ayeaye.PartitionedModel):
    """
    Use a `PartitionedModel` to run :class:`ayeaye.Model` and :class:`ayeaye.PartitionedModel`s in
    parallel. Models are only run when their data dependencies are complete.

    This class must be subclassed and given a set of models to run. It will determine the order to
    run the models based on data intra-dependencies. These data dependencies are those declared at
    class level as :class:`ayeaye.Connect` resources.

    The model must be run with `ayeaye.connect_resolve.connector_resolver` set to include anything
    needed by the build. (@see :class:`ConnectorResolver`)

    @see :meth:`tests.test_model_partitioned.TestPartitionedModel.test_parallel_models_with_dependencies`
    for an example of this running.
    """

    # subclass must implement this. It's a set of models.
    models = None

    def __init__(self):

        super().__init__()

        self.models_lookup = {m.__name__: m for m in self.models}  # name -> class

        # this isn't strictly needed but might be useful to track and confirm
        # that everything has run
        self.remaining_models = copy.copy(self.models)

        # Graph of dependencies between models
        self.model_collection = ayeaye.ModelCollection(models=set(self.remaining_models))

        # sync between `partition_subtask_complete` and `partition_slice`
        self.condition = Condition()
        self.running_models = set()
        self.recently_completed_models = set()

    def partition_slice(self, _slice_count):
        """
        Generator yielding :class:`TaskPartition` messages describing a model to run.

        This method will hang on a `Condition` whilst waiting for other models to complete.

        It becomes a generator so is separated out from messages sent to `partition_subtask_complete`.
        """
        while True:
            if not self.condition.acquire(True, timeout=3):
                continue

            if len(self.running_models) == 0 or self.condition.wait(timeout=1):

                just_completed = set()
                for model_name in self.recently_completed_models:
                    model_cls = self.models_lookup[model_name]
                    just_completed.add(model_cls)

                    self.remaining_models.discard(model_cls)
                    self.running_models.discard(model_name)

                # all used
                self.recently_completed_models.clear()

                ready_to_run = self.model_collection.ready_to_run(just_completed=just_completed)
                for model_cls in ready_to_run:

                    if model_cls in self.running_models:
                        # already running, model_collection only knows about complete models
                        continue

                    model_name = model_cls.__name__
                    self.log(f"Sending TaskPartition message for {model_name}", "DEBUG")
                    self.running_models.add(model_cls)
                    t = TaskPartition(
                        model_cls=model_cls,
                        method_name="go",  # full model runs from this method
                    )
                    yield t

            # wait() will re-acquire the lock both on receiving a notification and on timeout so
            # release it in both cases
            self.condition.release()

            if len(self.remaining_models) == 0:
                self.log("All models have been started", "DEBUG")
                return

        return

    def partition_subtask_complete(self, task_message):
        """
        Optional method. Called on the parent instance for each completed sub-task. It can be used
        to collate results or take further actions when a sub-task has finished.

        @param task_message: (:class:`ayeaye.runtime.task_message.TaskComplete`)
        """
        self.log(f"Model completed: {task_message.model_cls_name}")
        self.condition.acquire()
        self.recently_completed_models.add(task_message.model_cls_name)
        self.condition.notify()
        self.condition.release()
        return None

    def build(self):
        self.log("Running dependency driven model runner")


class A(ayeaye.Model):
    def build(self):
        from_context = ayeaye.connector_resolver.resolve("From the build context: {greeting}")
        self.log(f"This is Model A with context: {from_context}")


class B(ayeaye.Model):
    def __init__(self, init_arg, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.init_arg = init_arg

    def build(self):
        self.log(f"This is Model B with init arg: {self.init_arg}")


class C(ayeaye.Model):
    def build(self):
        self.log("This is Model C")


class ExampleModelRunner(AbstractModelRunner):
    models = [
        (A, {}),
        (B, {"init_arg": "hi model B"}),
        (C, {}),
    ]


if __name__ == "__main__":
    # run it from the command line like this-
    # $ pipenv shell
    # $ python examples/parallel_model_runner/model_runner.py

    build_context = {"greeting": "Hello command line model!"}
    with ayeaye.connector_resolver.context(**build_context):
        m = ExampleModelRunner()
        m.go()
