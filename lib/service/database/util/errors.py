import pandas as pd
from logging import Logger
from psycopg.errors import Error, NumericValueOutOfRange

def log_exception_info(logger: Logger, error: Error):
    logger.error(f"Args: {error.args}")
    d = error.diag
    logger.error(f"Diag.SchemaName: {d.schema_name}")
    logger.error(f"Diag.TableName: {d.table_name}")
    logger.error(f"Diag.ColName: {d.column_name}")
    logger.error(f"Diag.SqlState: {d.sqlstate}")
    logger.error(f"Diag.MsgPrimary: {d.message_primary}")
    logger.error(f"Diag.MsgDetail: {d.message_detail}")
    logger.error(f"Diag.MsgHit: {d.message_hint}")
    logger.error(f"Diag.StatementPos: {d.statement_position}")
    logger.error(f"Diag.InternalPos: {d.internal_position}")
    logger.error(f"Diag.InternalQuery State: {d.internal_query}")
    logger.error(f"Diag.Context: {d.context}")
    logger.error(f"Diag.SourceFile: {d.source_file}")
    logger.error(f"Diag.SourceLine: {d.source_line}")
    logger.error(f"Diag.SourceFunc: {d.source_function}")
    match error.pgresult:
        case res if res is not None:
            logger.error(f"Result.Msg: {res.get_error_message()}")
            logger.error(f"Result.Size: {res.ntuples}, {res.nfields}")
    logger.exception(error)

def log_exception_info_df(df: pd.DataFrame, logger: Logger, error: Error):
    with pd.option_context('display.max_columns', None):
        logger.error(str(df.head()))
    logger.error(df.info())
    logger.error(f"Columns: {df.columns}")
    logger.error(f"Rows: {len(df)}")
    log_exception_info(logger, error)
    match error:
        case NumericValueOutOfRange():
             df_num = df.select_dtypes(include=["number"])
             logger.error(f"summary stats\n{df_num.describe()}")

