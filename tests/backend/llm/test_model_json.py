import pytest

from pydantic import BaseModel, Field
from typing import Literal, get_args

from base.config import TEST_LLM
from base.core.values import as_json
from base.models.content import ContentText
from base.resources.aff_body import ObsBody
from base.resources.observation import Observation
from base.strings.auth import UserId

from backend.data.llm_models import get_llm_by_name, LlmModelName
from backend.llm.message import LlmPart, LlmText

from base.strings.resource import ObservableUri
from tests.backend.utils_context import given_headless_process
from tests.data.samples import given_sample_media


async def _callback_noop(messages: list[LlmPart]) -> None:
    pass


##
## Example - Documents
##


class LotoQuebecAnswer(BaseModel):
    lotto649_goldball_jackbot_millions: int = Field(
        description="The Lotto 6/49 La Boule d'Or jackbot in millions of dollars."
    )
    lottomax_jackbot_millions: int = Field(
        description="The Lotto Max jackbot in millions of dollars."
    )
    lottomax_maxmillions_jackbot_millions: int = Field(
        description="The Max Millions jackbot in millions of dollars or zero when not available."
    )


def _given_lotoquebec_observations() -> list[Observation]:
    return [
        ObsBody(
            uri=ObservableUri.decode("ndk://stub/-/lotto-6-49/$body"),
            description=None,
            content=ContentText.parse(
                """\
# On ne laisse pas la protection de votre vie privée au hasard

Nous utilisons des témoins (*cookies*) pour améliorer votre expérience de navigation sur nos sites, diffuser des publicités ou des contenus personnalisés et analyser la fréquentation de nos sites. En cliquant sur Tout accepter, vous consentez à ce que nous utilisions tous les types de témoins.

Pour en savoir plus, consultez notre [Politique de confidentialité](https://societe.lotoquebec.com/fr/confidentialite-app).

Voir nos partenairesPersonnaliser mes choixTout accepter![Lotto 6/49](self://lg-649.png)

Ce mercredi, 7 janvier 2026

La Boule d'or 18 million$\\*

Le Classique 5 million$

\\*ou 1 million

[Créer un abonnement](https://loteries.espacejeux.com/lel/fr/miser/lotto649/abonnement)[Jouer en groupe](https://loteries.espacejeux.com/lel/fr/miser/lotto649/groupe)\
""",
            ),
            sections=[],
            chunks=[],
        ),
        ObsBody(
            uri=ObservableUri.decode("ndk://stub/-/lotto-max/$body"),
            description=None,
            content=ContentText.parse(
                """\
# On ne laisse pas la protection de votre vie privée au hasard

Nous utilisons des témoins (*cookies*) pour améliorer votre expérience de navigation sur nos sites, diffuser des publicités ou des contenus personnalisés et analyser la fréquentation de nos sites. En cliquant sur Tout accepter, vous consentez à ce que nous utilisions tous les types de témoins.

Pour en savoir plus, consultez notre [Politique de confidentialité](https://societe.lotoquebec.com/fr/confidentialite-app).

Voir nos partenairesPersonnaliser mes choixTout accepter![Lotto Max](self://lg-lottomax-blanc.png)

Ce mardi, 6 janvier 2026

50 million$ + 2 Maxmillions\\*

\\*Approx.

[Créer un abonnement](https://loteries.espacejeux.com/lel/fr/miser/lottoMax/abonnement)[Jouer en groupe](https://loteries.espacejeux.com/lel/fr/miser/lottoMax/groupe)\
""",
            ),
            sections=[],
            chunks=[],
        ),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("model", "mode"),
    [
        (model, mode)
        for mode in ("batch", "stream")
        for model in get_args(LlmModelName)
        if (mode != "stream" or get_llm_by_name(model).supports_stream)
    ],
)
@pytest.mark.skipif(not TEST_LLM, reason="LLM tests disabled by default")
async def test_get_completion_json_with_documents(
    mode: Literal["batch", "stream"],
    model: LlmModelName,
):
    llm = get_llm_by_name(model)
    observations = _given_lotoquebec_observations()
    answer, _ = await llm.get_completion_json(
        process=given_headless_process(observations=list(observations)),
        callback=_callback_noop if mode == "stream" else None,
        system="""\
I need to know the CURRENT Loto-Quebec jackpots.
Your answer should only use the attached documents.
For example, a "50 000 000 $" jackpot becomes "50" in `<value>`.
If you can't find a value with certainty, then set it to zero. \
It should appear in the documents: do not guess or calculate it otherwise.\
""",
        messages=[
            LlmText.prompt(
                UserId.stub(),
                "\n\n".join(
                    [
                        "<documents>",
                        *[f"![]({obs.uri})" for obs in observations],
                        "</documents>",
                    ]
                ),
            ),
        ],
        type_=LotoQuebecAnswer,
    )
    print(f"<answer>\n{as_json(answer)}\n</answer>")

    assert isinstance(answer, LotoQuebecAnswer)
    assert answer.lotto649_goldball_jackbot_millions == 18
    assert answer.lottomax_jackbot_millions == 50
    assert answer.lottomax_maxmillions_jackbot_millions == 2


##
## Example - Image
##


class MusicPlayerAnswer(BaseModel):
    artist: str = Field(description="The name of the artist.")
    track: str = Field(description="The name of the track.")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("model", "mode"),
    [
        (model, mode)
        for mode in ("batch", "stream")
        for model in get_args(LlmModelName)
        if (mode != "stream" or get_llm_by_name(model).supports_stream)
    ],
)
@pytest.mark.skipif(not TEST_LLM, reason="LLM tests disabled by default")
async def test_get_completion_json_with_image(
    mode: Literal["batch", "stream"],
    model: LlmModelName,
):
    llm = get_llm_by_name(model)
    media = given_sample_media()
    answer, _ = await llm.get_completion_json(
        process=given_headless_process(observations=[media]),
        callback=_callback_noop if mode == "stream" else None,
        system=None,
        messages=[LlmText.prompt(UserId.stub(), f"![]({media.uri})")],
        type_=MusicPlayerAnswer,
    )
    print(f"<answer>\n{as_json(answer)}\n</answer>")

    assert isinstance(answer, MusicPlayerAnswer)
    assert answer.artist.lower() == "jonas brothers"
    assert answer.track.lower() == "sucker"
