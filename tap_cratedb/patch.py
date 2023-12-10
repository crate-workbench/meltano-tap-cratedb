import datetime as dt


def patch_sqlalchemy_dialect():
    patch_types()
    patch_datetime()
    patch_get_pk_constraint()


def patch_datetime():
    """
    The test suite will supply `dt.date` objects, which will
    otherwise fail on this routine.
    """

    from crate.client.sqlalchemy.dialect import DateTime

    def bind_processor(self, dialect):
        def process(value):
            if isinstance(value, (dt.datetime, dt.date)):
                return value.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            else:
                return value
        return process

    DateTime.bind_processor = bind_processor


def patch_get_pk_constraint():
    """
    Convert from `set` to `list`, to work around weirdness of the Python dialect.

    tap = TapCrateDB(config=SAMPLE_CONFIG)
    tap_catalog = json.loads(tap.catalog_json_text)

    TypeError: Object of type set is not JSON serializable
    """
    from sqlalchemy.engine import reflection
    from crate.client.sqlalchemy import CrateDialect

    get_pk_constraint_dist = CrateDialect.get_pk_constraint

    @reflection.cache
    def get_pk_constraint(self, engine, table_name, schema=None, **kw):
        outcome = get_pk_constraint_dist(self, engine, table_name, schema=schema, **kw)
        outcome["constrained_columns"] = list(outcome["constrained_columns"])
        return outcome

    CrateDialect.get_pk_constraint = get_pk_constraint


def patch_types():
    """
    Emulate PostgreSQL's `JSON` and `JSONB` types using CrateDB's `OBJECT` type.
    """
    from crate.client.sqlalchemy.compiler import CrateTypeCompiler

    def visit_JSON(self, type_, **kw):
        return "OBJECT"

    def visit_JSONB(self, type_, **kw):
        return "OBJECT"

    CrateTypeCompiler.visit_JSON = visit_JSON
    CrateTypeCompiler.visit_JSONB = visit_JSONB
