import random
import pytest
import pandas as pd
import numpy as np

from .main import (
    extract_sampleid,
    flag_viable,
    DEFAULT_EXCLUDE_CONDITIONS,
    DEFAULT_EXCLUDE_MATCODES,
)


@pytest.fixture
def specimen_df():
    return SpecimenDF(
        [
            Specimen(
                comments="SAMPLEID:12345, Other info",
                matcode="10x10Box",
                receivedcondition="",
                sample_condition="",
            ),
            Specimen(
                comments="No sample id here",
                receivedcondition="NSI",
                sample_condition="",
            ),
        ]
    )


class Specimen:
    def __init__(
        self,
        comments="",
        amountleft=random.randint(0, 500),
        matcode=random.choice(DEFAULT_EXCLUDE_MATCODES + ["10x10Box", "9x9Box"]),
        receivedcondition=random.choice(DEFAULT_EXCLUDE_CONDITIONS + [""]),
        sample_condition=random.choice(DEFAULT_EXCLUDE_CONDITIONS + [""]),
    ):
        self.Comments = comments
        self.AMOUNTLEFT = amountleft
        self.MATCODE = matcode
        self.RECEIVEDCONDITION = receivedcondition
        self.__dict__["Sample Condition"] = sample_condition


class SpecimenDF:
    def __init__(self, specimens):
        self.df = pd.DataFrame([specimen.__dict__ for specimen in specimens])


def test_extract_sampleid(specimen_df):
    extract_sampleid(specimen_df.df)
    assert specimen_df.df["SAMPLEID"].iloc[0] is not None
    assert specimen_df.df["SAMPLEID"].iloc[0] == "12345"
    assert specimen_df.df["SAMPLEID"].iloc[1] is np.nan


def test_flag_viable(specimen_df):
    flagged_df = flag_viable(specimen_df.df)
    # Specimen with valid matcode and no exclude conditions should be viable
    # Specimen with NSI condition should be non-viable
    assert flagged_df["VIABLE"].tolist() == [True, False]
