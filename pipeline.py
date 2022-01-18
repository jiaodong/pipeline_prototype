import threading
from typing import Any, Dict, List
import asyncio

from ray import serve

Configurable = Any

"""
CUJ:

Dev Team:
 - Mostly deal with code

    1) Write python code with just classes
    2) Run python script with classes
    3) Annotate classes to scale with @serve.deployment
    4) Test it in local / remote ray cluster via MyClass.deploy(recursive=True)
    5) Everything looks good, call build / generate mode to generate a
        self-contained (?) yaml file
    5.5) packaing / runtime env
    6) [Optional] Use CLI to deploy the yaml file to production

Ops Team:
 - Mostly deal with configs / ops without knowing or mutating code

    1) Get the yaml file from Dev or call into CLI to query the yaml
    2) Yaml file should represent target state and ground truth of current
        deployment / pipeline
    3) Mutate fields on the yaml file, such as system configs (num_replicas),
        or runtime variables (weight = 1 -> weight = 2)
    4) Use CLI to redeploy yaml file and apply changes to existing pipeline


Single deployment code change:
 - Dev Team:
    1) Change code and verify it works on ray cluster
    2) Manually bump deployment verison @serve.deployment(version="v2")
    3) Generated new deployment yaml where version field changed
 - Ops Team:
    1) Just apply the new yaml with right code packaging, ideally self-contained
        format of deployment.

TBD: Packaing of deployment code and runtime_env, extend to pipeline

Multiple deployments coed chagne (pipeline, update in tandem):
  - Dev Team:
    1) Same as single case, except multiple classes are changed and bumped
        version
  - Ops Team:
    1) Just apply the new yaml with right code packaging, ideally self-contained
        format of deployment.
  - Serve:
    1) Control plane needs to know dependencies to kick off updates in right
        order, or fully async for each one
    2) Data plane needs to redirect traffic to right deployments while multiple
        versions of a deployment exist

"""

# Pipeline nodes are written from leaf to root (entrypoint)
@serve.deployment
class Model:
    # For backwards compatibility
    _version: int = 1

    # Note there's ZERO pipeline logic here on purpose, just focus on the model
    # Can also be instantiated multiple times with different weights, under
    # same class def & implementation.
    def __init__(self, weight):
        self.weight: Configurable = weight
        self.policy: Configurable = 1

    def reconfigure():
        pass

    async def __call__(self, req):
        return req * self.weight

@serve.deployment
class FeatureProcessing:
    # For backwards compatibility
    _version: int = 1

    def __init__(self):
        pass

    async def __call__(self, req):
        return max(req, 0)

@serve.deployment
class Pipeline:
    # For backwards compatibility
    _version: int = 1

    def __init__(
        self,
        a: DeploymentHandle / Deployment,
        b: FeatureProcessing = DependsOn,
        c, d ...
    ):
        self.feature_processing = FeatureProcessing()
        self.lock = threading.Lock()

        # TODO: Add a pipeline container here so we can keep this implementation
        # but also make nodes registered with unique name for each instance
        # self.models = [Model(i) for i in range(3)]

        self.model_1 = Model(1) # What if this is heavy .. use a stub ?
        self.model_2 = Model(2)

    async def __call__(self, req):
        """
        1) No ray API knowledge is required here, user just provides blocks of
            code. There's even no ray API call made.
        2) Underlying communication is handled by us, where we can decide how
            to make ray actor calls, to which group, on which node.
        3) For scaling and updates, user can opt-in serve deployment as an
            executor type with more dynamic support. Since we know the DAG, we
            can make right update in tandem calls while redirecting traffic
            accordingly on the right path.
        """
        processed_feature = await self.feature_processing(req)

        if processed_feature < 5:
            x = await self.model_1(processed_feature)
        elif processed_feature >= 5 and processed_feature < 10:
            x = await self.model_2(processed_feature)
        else:
            x = await self.model_3(processed_feature)

        return x

    # def __repr__(self):
    #     """
    #     Return pretty printted nested nodes.
    #     """
    #     pass

    def resize(self, node_name: str):
        pass

    def reconfigure(self, new_config: Dict[str, Any]):
        pass

    def update(self, node_name: str, serialized_class_callable: bytes):
        pass


"""
Another example where RequestManager takes input from B and C
"""

class SpamChecker:
    def __init__(self):
        pass

    def __call__(self, req) -> List:
        return [req + 1, req + 2]
class ImportantChecker:
    def __init__(self):
        pass

    def __call__(self, req) -> List:
        return [req + 2, req]

class Aggregator:
    def __init__(self):
        pass
    def __call__(self, req: List):
        return sum(req)

@serve.deployment
class RequestManager:
    def __init__(self):
        self.is_spam = SpamChecker()
        self.is_important = ImportantChecker()
        self.aggregator = Aggregator()

        """
        We can add serve modules that takes collection of serve handles and
        optimize for most commonly used patterns to reduce uncessary data
        fetching / ray.get, such as
        https://docs.ray.io/en/master/ray-design-patterns/index.html

            - Chaining
                ray.get(a.remote(b.remote(c.remote(req))))
            - Broadcast & Reduce
                ray.get([req_refs])
        """
        # serve.Broadcast(self, [b, c])
        # serve.Reduce([b, c], d)
    @staticmethod
    def build(config):


    def __call__(self, req):
        # ref = ray.remote(ray.remote(...))
        # ray.get(ref)
        return self.aggregator(
            self.is_spam(req) + self.is_important(req)
        )


# ===========================================================================

async def main():
    # Solve node init / instantiate
        # maybe just dummy task ?19
    # Solve node DAG tracing
        # See if we can avoid making symbolic calls with pipeline.INPUT
    # Add pprint strings
    # Can be called
    pipeline = Pipeline()
    # for i in range(10):
    #     print(await pipeline(i))
    """
    Output:
    0
    1
    2
    3
    4
    10
    12
    14
    16
    18
    """
    # Recursively do:
    # add executor for self
    # traces other nodes as instance variables of my class, annotated as "step"
    # add to my node's dictionary
    # pipeline.instantiate(recursive=True)

    serve.start()
    Pipeline.deploy(recursive=True)
    pipeline = serve.api._get_global_client().get_handle("Pipeline")
    for i in range(10):
        print(await pipeline(i))
    # print("AA")
if __name__ == "__main__":
    asyncio.run(main())




####### TANMAY'S PROPOSAL #######
yaml:
Preprocessor1:
    runtime_env: ...
    class: my_file:Preprocessor
    args: [1]
    num_replicas: 10
Preprocessor2:
    runtime_env: ...
    class: my_file:Preprocessor
    args: [2]
    num_replicas: 10
Pipeline:
    runtime_env: ...
    class: my_file:Pipeline
    num_replicas: 10

class Pipeline:
    def __init__(self,
        preprocessor1: Preprocessor = depends("Preprocessor1")
        preprocessor2: serve.Deployment["Preprocessor2"]
        all_preprocessors: List[Preprocessor] = depends([Preprocessor for i in range(100)], args={})
    ):
        self.other_class = OtherClass()



###### EOAKES PROPOSAL #######

@serve.deployment
class A:
    def __init__(self, model_url):
        # ./model.pkl is *only* available on the cluster, not on my laptop.
        self._my_model = load_model(model_uri)

@serve.deployment
class B:
    def __init__(self, a: A):
        pass

@serve.deployment
class C:
    def __init__(self, all_the_models: List[Deployment]):
        self._sequential = serve.Sequential(all_the_models)

    def __call__(self):
        pass # Do whatever with a and b.

a = A(uri)
b = B(a) # Bind operation only, doesn't run constructor.
c = C(b)

def setup_dag():
    a = A(uri)
    b = B(a) # Bind operation only, doesn't run constructor.
    c = C(b)
    return c # c.build()

models = [C.bind(i) for i in range(1000)]
c = C({"models": models})

class D:
    def __init__(models = depends(models)):


####### Simon note ###
# dependency relationship: global and local
    # global: global dag builder
        # + global view
        # - imperative logic?
    # local: useful for individual testing and whatever benefit of dep injection
# Data path: localized?
    # the top level driver deployment (C in Ed's case) shows the data path in __call__.
########

##### tanmay proposal #####


@serve.deployment
class A:
    def __init__(self, model_url):
        # ./model.pkl is *only* available on the cluster, not on my laptop.
        self._my_model = load_model(model_uri)

    def __build__(self):
        pass

@serve.deployment
class B:
    def __init__(self, a: A):
        pass

    def __build__(self, input):
        A = ray.get_handle("name")
        return A(input)

@serve.deployment
class C:
    def __init__(self, all_the_models: List[Deployment]):
        self._sequential = serve.Sequential(all_the_models)

    def __call__(self):
        pass # Do whatever with a and b.


c.run()
c.build()




class MyPipeline:
    def __init__(self) -> None:
        self.a = A(arg1)
        self.b = B(arg2)
        self.c = C(arg3)

class MyPipeline:
    # 1. Where do arg1,2,3 go?
    # 2. escape hatch programmatic construction: needs to be super smooth transition.
    def __init__(self, a: A, b: B, c:C) -> None:
        self.a = A
        self.b = B
        self.c = C

# All of the following can be ran in <1s. (import cost)
import inspect
inspect.getfullargspec(MyPipeline.__init__)
inspect.getfullargspec(A.__init__)
inspect.getfullargspec(B.__init__)

# super fast
# serve pipeline build-yaml my-dir

###############

@serve.deployment
class D: pass

@serve.deployment
class MyPipeline:
    depends = [A, B, C]
    a.bind(1)
    def __init__(self, a: A):
        self.a = a

        # This will error, because we control MyPipeline.init and D.init
        D()

with mock.Mock("my_package.A", "__call__") as m:
    m.return_value = "hello"
    MyPipeline.deploy().call()


@serve.deployment
class MyPipeline:
    def __init__(self) -> None:
        if not serve.IN_BUILD_PHASE:
            self.model_file = download_model_to_disk()
        else:
            self.model_file = "dummy"

################


@serve.deployment
class Pipeline:
    # For backwards compatibility
    _version: int = 1

    def __init__(
        self,
        a: DeploymentHandle / Deployment,
        b: FeatureProcessing = DependsOn,
        c, d ...
    ):
        self.feature_processing = FeatureProcessing()
        self.lock = threading.Lock()

        # TODO: Add a pipeline container here so we can keep this implementation
        # but also make nodes registered with unique name for each instance
        # self.models = [Model(i) for i in range(3)]

        self.model_1 = Model(1) # What if this is heavy .. use a stub ?
        self.model_2 = Model(2)

    async def __call__(self, req):
        ...

    # def __repr__(self):
    #     """
    #     Return pretty printted nested nodes.
    #     """
    #     pass

    def resize(self, node_name: str):
        pass

    def reconfigure(self, new_config: Dict[str, Any]):
        pass

    def update(self, node_name: str, serialized_class_callable: bytes):
        pass


########

class Pipeline:
    def __init__(self, a: A, b: B):
        ...


Pipeline()

self.a = A()
self.b = B()

Pipeline(a=(1, 2))

self.a = A(1, 2)
self.b = B()



###
# with serve, inject deployment classes, BUT we deploy the models ahead of time
# ensure they exist on replica, but don't run the init.
# pipeline will have a deploy/allocate phase, and all of them will run the init in
# in tree order.


# testing:
# Pipeline(MockA, MockB)
class MockA:
    def __init__(self, *args): pass
class ActualA: # instantiate A in the same process as Pipeline
    def __init__(self): pass

# init args, programmatic construction, etc

# downside: pass in python class as argument?
# downside: two phase (?)
# downside: can't share instance of dependency



@serve.deployment(num_replicas=2)
class Pipeline:
    def __init__(self, feat_process_cls: A, model_cls: B):
        self.a = feat_process_cls(random.random()) # what happen in actual A?
        self.b = model_cls(arg2)


class SpamConfig(pydantic.Model):
    model_path: str

class MyPipelineConfig(pydantic.Model):
    spam_config_list: List[SpamConfig]


# use __init__ for constructor.
    # self.x fields in init can be anything
# always use cls.build for nested deployments.
    # self.x are all children deployments (handles).
# use reconfigure to runtime-updates.
    # self.x fields in reconfigure is update/override.

@serve.deployment
class SpamChecker:
    def __init__(self, config: SpamConfig):
        self.build() # pass in the config?



        # that's it



@serve.deployment
class RequestManager:
    def __init__(self, config):
        # must only run in replica
        self.contnt = download_url(config.url)

        self.__build__()

    def build(self, a: A=None, b_1: B=None, b_2: B=None):
        if a is None
            self.a = A(1,2) -> RequestManager.bind.(1, 2)
                A.build()
        else:
            a = A
        self.b_1 = B(3)
        self.b_2 = B(4)

    def __call__(self, req):
        self.a(
            self.b(req) + self.c(req)
        )

    def build(self, config: MyPipelineConfig):
        # can be ran anywhere.
        # but will be ran after init.
        self.is_spam_lst = [
            SpamChecker(c) # c is just a str
            for c in config.spam_config_list
        ]

    # Serve will call __build__ to figure out the graph dependencies
    # Serve will call __init__ and then __build__ when actually deploying your pipeline.


# once per config change
# serve pipeline build-template -> config_template.yaml

# once per config update / code update.
# serve pipeline build-pipeline config.yaml -> pipeline.lock.json
# serve deploy pipeline.lock.json
{
    "RequestManager": {
        "is_spam_lst": [
            {
                "SpamChecker": {
                    "model_path": "model_path_1"
                }
            },
            {
                "SpamChecker": {
                    "model_path": "model_path_2"
                }
            },
        ]
        }
    }
}


# --- walks the build method and construct the dag.

#############

@serve.deployment
class SpamChecker:
    def __init__(self, config: SpamConfig):
        self.build() # pass in the config?

    def build(self, path: str):
        self.model_path = path

@serve.deployment
class MyPipeline:
    # def run_time_constructor
    def __init__(self, config):
        self.other_stuff = config.url

    # def compile_time_constructor
    def build(self, config):
        self.inner_models = [
            SpamChecker.build(path)
            for path in config.paths
        ]


SpamChecker.build(path)

# unit testing:
# p = MyPipeline()
# # instead of p.build()
# p.inner_models = MyModels()
# ...


# config.yaml
# paths: [path_1, path_2]

# serve build config.yaml > serve_app.json
# {
#     "config.yaml": "copy of config.yaml",
#     "MyPipeline": {
#         "attrs": {
#             "key": "inner_models",
#             "value": [
#                 {"SpamChecker": {"attrs": {"key": "model_path", "value": "path_1"}}}
#                 {"SpamChecker": {"attrs": {"key": "model_path", "value": "path_2"}}}
#             ]
#         }
#     }
# }

# serve deploy serve_app.json

# deploying... SpamChecker.0
    # setting self.model_path = "path_1"
# deploying... SpamChecker.1
    # setting self.model_path = "path_2"
# deploying... MyPipeline
    # setting self.inner_models = [serve.get_handle(name) for name in ["SpamCheker.0", "SpamChecker.1"]]


from functools import partial
import inspect

# placeholder
class Featurizer:
    def __init__(self):
        pass

    @classmethod
    def build(*args, **kwargs):
        sig = inspect.signature(Featurizer.__init__)
        # Works like init but instead of executing, bind init args
        return sig.bind(*args, **kwargs)


class SpamFinder:
    def __init__(self):
        pass

    @classmethod
    def build(*args, **kwargs):
        # Works like init but instead of executing, bind init args
        return partial(SpamFinder.__init__, *args, **kwargs)

class Pipeline:
	# Handles are injected at runtime.
	def __init__(
        self,
        my_other_init_args,
        model: Featurizer,
        downstream: List[SpamFinder]
    ):
        # Variable name and arg name doesn't need to be the same
        # user decides here.
		self.model_1 = model
		self.downstreams = downstream

    @staticmethod
	def build(config):
        # Figure out dependencies without actually instantiating anything
		# return the things to be injected into Pipeline

		# direct tuples will be injected into *args
        with ray.dag.builder():
            m1 = Featurizer.remote(config.featurizer_config)
            downstream_list = [SpamFinder.remote(path) for path in config.model_paths]

		return m1, downstream_list

    def forward(self, req):
        if req > 0:
            return self.model_1 + self.downstreams[0]
        else:
            return self.model_1 + self.downstreams[1]

    def deploy():
        pass

===================


@serve.deployment(num_replicas=2)
class CachedModel():
    def __init__(self, arg):
        self.model = Model(arg)

    def forward(self, x):
        return self.model(x)

    async def __call__(self, x):
        return await self.forward(x)

@serve.deployment(num_replicas=2)
class Selector():
    def __init__(self, arg, m1, m2):
        self.model = Model(arg)
        self.m1 = m1
        self.m2 = m2

    async def forward(self, req):
        intermediate_rst = await self.model(req)
        if intermediate_rst == 0:
            return await self.m1(req)
        else:
            return await self.m2(req)

        # Another possible communication pattern
        return await self.model(
            await self.m1(req) + await self.m2(req)
        )

    async def __call__(self, req):
        return await self.forward(req)

with ray.dag.builder() as sample_in:
     m1 = CachedModel.remote(1) # bind init arg
     m2 = CachedModel.remote(2) # bind init arg
     selector = Selector.remote(3, m1, m2) # bind init arg + serve handles

     dag = selector.forward.remote(sample_in)

# Get whole DAG using Selector as root node
dag_def = serve.pipeline(dag).build()
print(dag_def) -> YAML / PrettyPrinted DAG with dependencies + args
"""
selector:
    class: my_module.Selector
    num_replicas: 2
    init_args: 3

m1:
    class: my_module.Selector
    num_replicas: 2
    init_args: 1
m2:
    class: my_module.Selector
    num_replicas: 2
    init_args: 2

==== Immutable Config====
DAG:
    selector -> m1
            |
            -> m2
"""


# If I want to extend and re-use existing classes, ideally without knowing
# its implementation in detail
@serve.deployment(num_replicas=2)
class ExtendedClass():
    def __init__(self, selector, m3):
        self.selector = selector
        self.m3 = m3

    def forward(self, req):
        if self.m3(req) == 0:
            return self.selector(req)
        else:
            return 0

    async def __call__(self, req):
        return await self.forward(req)

# Then extend the builder underneath
with ray.dag.builder() as sample_in:
     m1 = CachedModel.remote(1) # bind init arg
     m2 = CachedModel.remote(2) # bind init arg
     selector = Selector.remote(3, m1, m2) # bind init arg + serve handles
     m3 = CachedModel.remote(3)
     extended = ExtendedClass.remote(selector, m3)

     dag = extended.forward.remote(sample_in)

==========================================================================================
class MyModel

@serve.deployment(num_replicas=2) # Later will be -> @ray.remote(num_replicas=2)
class Model:
    def __init__(self, weights):
        self.model = MyModel(weights)

    def forward(self, input):
        return self.model(input)

@serve.deployment # Later will be -> @ray.remote
class ModelSelection:
    def __init__(self, m1: ActorHandleLike, m2: ActorHandleLike, threshold = 5):
       self.m0 = Model(np.ones(5, dtype=np.float128))
       self.m1 = m1 # Fulfilled at runtime, can be mock objects for testing
       self.m2 = m2 # Fulfilled at runtime, can be mock objects for testing

       self.threshold = threshold # This meant to be configurable

    async def forward(self, req):
        if req < self.threshold:
            return await self.m0(
                # We can optionally introduce operators to optimize most
                # commonly appeared operation pattern to reduce intermediate
                # data transfer
                await self.m1(req) + await self.m2(req)
            )
        else:
            return await self.m0(
                await self.m1(req) * await self.m2(req)
            )

with ray.dag.builder() as sample_in:
    m1 = Model.remote(np.zeros((2, 2))) # Different init args bounded for same class
    m2 = Model.remote(np.ones((2, 2))) # Different init args bounded for same class
    selection = ModelSelection.remote(
        m1, m2,      # ActorHandleLikes are bounded at build time
        threshold=10 # All the rest of init args/kwargs are also bounded for init
                     # but can be configurable by applying YAML changes.
    )
    dag = selection.forward.remote(sample_in)

# Locally evaluate on a single sample.
dag.call(sample) -> ModelOutput

# `artifact` is a pickleable, potentially non-human-readable blob.
# `config` is a human-readable YAML that contains dynamic options (e.g., num_replicas for each model).
artifact, config = dag.build(
    runtime_env = {
        "working_dir"= "./working_dir",
        "pip": ["requests==2.26.0"]
    }
)

"""
====Artifact=====
DeploymentHandles:
    m1:
        class: working_dir.Model
        args: (np.zeros((2, 2)),) # what if this is other object type or not serializable ?
    m2: class: working_dir.Model
        args: (np.ones((2, 2)),)
    selection:
        class: working_dir.ModelSelection
        args: (m1, m2, 10)
        deps: [m1, m2]

DAG: (Just for UX)
                         -> m1 (Model)(np.zeros((2, 2)),)
                        /
         selection
      (ModelSelection)
    (m1,m2,threshold=10)
                        \
                         -> m2 (Model)(np.ones((2, 2)),)

Entrypoint:
    selection.forward

Runtime_env:
    working_dir:
        "./working_dir"
    pip:
        requests: "2.26.0"
"""

"""
====Mutable config, first generated from code, can be mutated later on====
m1:
    num_replicas:2
m2:
    num_replicas:5
selection:
    num_replicas:6
    threshold: 7   --> This overrides bounded arg

HTTPOptions:  --> Since I didn't give one, just generate all default values
    host: "127.0.0.1"
    port: "8000"
    middlewares: []
    location: "HeadOnly"
    num_cpus: 0
    root_url: ""
    fixed_number_replicas: None
    fixed_number_selection_seed: 0
"""

serve_dag_handle = dag.deploy(artifact, config)
serve_dag_handle.remote(input)


# Can also work with functions
# @serve.deployment # Later will be -> @ray.remote
# async def decide(x, method1, method2):
#       if x % 2 == 0:
#           return await method1.remote(x)
#       else:
#           return await method2.remote(x)

# @ray.remote
# def model1(x):
#     return x + 1

# @ray.remote
# def model2(x):
#     return x + 2

# with ray.dag.builder() as sample_in:
#      dag = decide.remote(sample_in, model1, model2)


# Evaluate on a single sample.
dag.call(sample) -> ModelOutput


# `artifact` is a pickleable, non-human-readable blob.
# `config` is a human-readable YAML that contains dynamic options (e.g., num_replicas for each model).
immutable_artifact, mutable_config = dag.build()



# Deploy as pipeline. The classes are created once and re-used.
serve.pipeline(dag).deploy()

# Alternatively do things via CLI:
serve build my_file:dag_builder_fn
serve deploy my_file:dag_builder_fn

# Update by changing and applying mutable configs (num_replicas, path to model file, etc.)
serve deploy my_file:dag_builder_fn --config=mutable_config



# Proposal:
# Artifact
#   Runtime_env with working_dir & dependencies
#   DAG IR (Groups in operationalizing serve doc)
# Mutable config
#   num of replicas
#   Reconfigurable variable specified by user

# Problem Space: Base DAG, A->B->C
#   DAG change: A->C
#       Proposed: redeploy
#   Code implementation change A->B’->C
#       Proposed: redeploy first
#   config change
#   User dependency change
# ============== below this should be taken care of by ray wheel ============
#   Ray lib dependency change
#   underlying docker, os or platform changes

# 1) All nodes in DAG have same runtime env
ray job submit --runtime_env={a} entrypoint="python dag_builder.py"


# Generates artifact and config to target location
serve build my_file:dag_builder_fn
    --runtime_env={"working_dir": "./working_dir", "pip": ["requests==2.26.0"]}
    --dest="."

serve deploy --artifact="./artifact" --config="./deployment_config.yaml"
