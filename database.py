from sqlalchemy import create_engine, Column, Integer, String, inspect, Float, Boolean, Date, DateTime
from sqlalchemy.orm import declarative_base
from logging_config import loggers
import os

# SQLAlchemy Base and Logger
Base = declarative_base()
log = loggers["db_logger"]
def delete_database(db_path='nhl_database.db'):
    if os.path.exists(db_path):
        os.remove(db_path)
        log.warning(f"Database '{db_path}' deleted.")
    else:
        log.warning(f"Database '{db_path}' does not exist.")

# Call this function before setting up the database
delete_database()

# Define Team Table
class teams(Base):
    __tablename__ = 'teams'
    team_id = Column(Integer, primary_key=True)
    franchise_id = Column(Integer, nullable=False)
    team_name = Column(String, nullable=False)
    name_code = Column(String, nullable=False)

# Define Team Yearly Performance
class team_season_performance(Base):
    __tablename__ = 'team_season_performance'
    franchise_id = Column(Integer, primary_key=True)
    season_start = Column(Integer, nullable=False)
    season_end  =  Column(Integer, nullable=False)
    regular_season_games = Column(Boolean, nullable=False)
    post_season_games = Column(Boolean, nullable=False)
    wins = Column(Integer, nullable = False)
    losses = Column(Integer, nullable = False)
    overtime_losses = Column(Integer, nullable = False)

class overall_season_info(Base):
    __tablename__  = 'overall_season_info'
    season_id = Column(Integer, primary_key=True)
    all_star_game_in_use = Column(Boolean, default=False)
    conferences_in_use = Column(Boolean, default=False)
    divisions_in_use = Column(Boolean, default=False)
    end_date = Column(DateTime)
    entry_draft_in_use = Column(Boolean, default=False)
    formatted_season_id = Column(String, nullable=False)
    minimum_playoff_minutes_for_goalie_stats_leaders = Column(Integer, default=0)
    minimum_regular_games_for_goalie_stats_leaders = Column(Integer, default=0)
    nhl_stanley_cup_owner = Column(Boolean, default=False)
    number_of_games = Column(Integer, default=0)
    olympics_participation = Column(Boolean, default=False)
    point_for_ot_loss_in_use = Column(Boolean, default=False)
    preseason_start_date = Column(DateTime, nullable=True)
    regular_season_end_date = Column(DateTime)
    row_in_use = Column(Boolean, default=False)
    season_ordinal = Column(Integer, default=0)
    start_date = Column(DateTime)
    supplemental_draft_in_use = Column(Boolean, default=False)
    ties_in_use = Column(Boolean, default=False)
    total_playoff_games = Column(Integer, default=0)
    total_regular_season_games = Column(Integer, default=0)
    wildcard_in_use = Column(Boolean, default=False)

# Setup Database and Check for Tables/Columns
def setup_database(db_url='sqlite:///nhl_database.db'):
    engine = create_engine(db_url)
    if not os.path.exists('nhl_database.db'):
        log.info("nhl_database.db Not Found... One will be created")
        Base.metadata.create_all(engine)
        log.info("Database and tables created successfully.")
    else:
        log.info("Database Found!")
    return engine

# Normalize types for comparison
def normalize_type(column_type):
    """
    Normalize SQLAlchemy column types to SQLite equivalents for comparison.
    """
    if isinstance(column_type, Integer):
        return "INTEGER"
    elif isinstance(column_type, String):
        return "VARCHAR"
    elif isinstance(column_type, Float):
        return "REAL"
    elif isinstance(column_type, Boolean):
        return "BOOLEAN"
    elif isinstance(column_type, Date):
        return "DATE"
    elif isinstance(column_type, DateTime):
        return "DATETIME"
    else:
        return str(column_type)  # Fallback for custom or uncommon types


# Compare Database to Model
def compare_db_to_model(engine, base):
    inspector = inspect(engine)
    for table_name, model_class in base.metadata.tables.items():
        # Check if the table exists in the database
        if table_name not in inspector.get_table_names():
            log.warning(f"Table '{table_name}' is missing. Creating table...")
            base.metadata.create_all(engine, tables=[model_class])
        else:
            # Check each column in the table
            db_columns = {col['name']: normalize_type(col['type']) for col in inspector.get_columns(table_name)}
            model_columns = {col.name: normalize_type(col.type) for col in model_class.columns}

            missing_columns = []
            mismatched_columns = []

            for column_name, column_type in model_columns.items():
                if column_name not in db_columns:
                    missing_columns.append(column_name)
                elif db_columns[column_name] != column_type:
                    mismatched_columns.append((column_name, db_columns[column_name], column_type))

            # Log missing columns
            if missing_columns:
                log.warning(f"Table '{table_name}' is missing columns: {missing_columns}")
                log.info(f"Consider using migration tools to add missing columns to '{table_name}'.")

            # Log mismatched columns (won't auto-correct types in SQLite without migration tools)
            if mismatched_columns:
                log.warning(f"Table '{table_name}' has mismatched column types: {mismatched_columns}")
                log.info(f"Consider using migration tools to fix column types in '{table_name}'.")

            # Confirm if no issues found
            if not missing_columns and not mismatched_columns:
                log.info(f"Table '{table_name}' structure is verified.")

# Initialize the database on startup
engine = setup_database()
compare_db_to_model(engine, Base)

