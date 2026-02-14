from dataclasses import dataclass

from base.models.content import ContentText
from base.models.context import NdCache
from base.models.rendered import Rendered
from base.resources.bundle import Resources
from base.resources.observation import Observation


@dataclass(kw_only=True)
class CacheResources(NdCache):
    resources: Resources

    @classmethod
    def initialize(cls) -> CacheResources:
        return CacheResources(resources=Resources())

    def render_content(self, text: ContentText) -> Rendered:
        """
        Render the content, expanding embeds recursively using the observations
        that are already cached in the workspace.
        """
        observations = [
            obs for obs in self.resources.observations if isinstance(obs, Observation)
        ]
        return Rendered.render(text, observations)
