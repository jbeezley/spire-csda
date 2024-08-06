from pydantic import BaseModel, ConfigDict, Field

from spire_csda.models.search import CSDASearch


class BulkDownload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    queries: list[CSDASearch] = Field(..., min_length=1)
