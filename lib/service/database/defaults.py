from .config import DatabaseConfig

# NOTE THIS DATABASE IS NOT PRIVATE OF INTENDED
# TO BE HOSTED ON ANY MACHINE AND SHOULDN'T BE
# TREATED AS ANYTHING OTHER THAN A FANCY FILE
# THAT JUST HAPPENS TO BE FAIRLY ORGANISED.
#
# If for any reason you choose to extend this
# application or encorporate it in a larger
# system, absolutely define credentials in a
# more confindential manner.

instance_1_config = DatabaseConfig(
    dbname='gnaf_db',
    user='postgres',
    host='localhost',
    port=5432,
    password='throwAwayPassword',
)

instance_2_config = DatabaseConfig(
    dbname='gnaf_db_2',
    user='postgres',
    host='localhost',
    port=5433,
    password='throwAwayPassword2',
)

