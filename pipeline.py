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

# @serve.deployment
class Pipeline:
    # For backwards compatibility
    _version: int = 1

    def __init__(self):
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