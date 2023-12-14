"""A Singer tap for CrateDB, built with the Meltano SDK, based on the PostgreSQL tap."""
from tap_cratedb.patch import patch_sqlalchemy_dialect

patch_sqlalchemy_dialect()
