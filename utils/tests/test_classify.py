import numpy as np
import pandas as pd
import pytest

from utils.classify import matcher

d = {
    "name": [
        "bob von rosevich",
        "anantarya smith",
        "bob j vonrosevich",
        "missy elliot",
        "mr johnson",
        "quarantin directino",
        "missy eliot",
        "joseph johnson",
    ],
    "address": [
        "3 Blue Drive, Chicago",
        "4 Blue Drive, Chicago",
        "8 Fancy Way, Chicago",
        "8 Fancy Way, Evanston",
        "17 Regular Road, Chicago",
        "42 Hollywood Boulevard, Chicago",
        "8 Fancy Way, Evanston",
        "17 Regular Road, Chicago",
    ],
}

test_df = pd.DataFrame(data=d)

test_df["classification"] = "neutral"


@pytest.fixture
def matcher_scen_1():
    return test_df


def test_matcher_scen_1(matcher_scen_1):
    matcher(matcher_scen_1, "Fancy", "address", "f")
    res = test_df[test_df["classification"] == "f"]["name"].values

    assert np.all(
        res == np.array(["bob j vonrosevich", "missy elliot", "missy eliot"])
    )
