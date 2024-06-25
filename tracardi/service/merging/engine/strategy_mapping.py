from tracardi.service.merging.engine.merging_strategy_types import LAST_UPDATE, FIRST_UPDATE, MIN, MAX, StrategyRecord, \
    SUM, AVG, LAST_DATETIME, FIRST_DATETIME, ALWAYS_TRUE, ALWAYS_FALSE, AND, OR, CONCAT, UNIQUE_CONCAT, \
    FIRST_PROFILE_INSERT_TIME, LAST_PROFILE_UPDATE_TIME, LAST_PROFILE_INSERT_TIME, FIRST_ITEM
from tracardi.service.merging.engine.strategy.bool_strategy import OrStrategy, AndStrategy, AlwaysFalseStrategy, \
    AlwaysTrueStrategy
from tracardi.service.merging.engine.strategy.datetime_strategy import MinDateTimeStrategy, MaxDateTimeStrategy
from tracardi.service.merging.engine.strategy.first_strategy import FirstStrategy
from tracardi.service.merging.engine.strategy.list_strategy import ConCatDistinctStrategy, ConCatStrategy
from tracardi.service.merging.engine.strategy.profile_datetime_strategy import LastProfileInsertTimeStrategy, \
    LastProfileUpdateTimeStrategy, FirstProfileInsertTimeStrategy
from tracardi.service.merging.engine.strategy.value_strategy import AvgValueStrategy, SumValueStrategy, MaxValueStrategy, \
    MinValueStrategy
from tracardi.service.merging.engine.strategy.value_update_strategy import LastUpdateStrategy, FirstUpdateStrategy

id_to_strategy = {

    LAST_UPDATE: StrategyRecord(
        id=LAST_UPDATE,
        name="Last updated value",
        description="This merge strategy use update date to find the most fresh that will prevail.",
        strategy=LastUpdateStrategy),
    FIRST_UPDATE: StrategyRecord(
        id=LAST_UPDATE,
        name="First inserted value",
        description="This merge strategy use update date to find the first inserted that will prevail",
        strategy=FirstUpdateStrategy),

    MIN: StrategyRecord(
        id=MIN,
        name="Minimal value",
        description="This merge strategy will select minimal value as merged value",
        strategy=MinValueStrategy
    ),
    MAX: StrategyRecord(
        id=MAX,
        name="Maximal value",
        description="This merge strategy will select maximal value as merged value",
        strategy=MaxValueStrategy),
    SUM: StrategyRecord(
        id=SUM,
        name="Sum of values",
        description="This merge strategy will sum all values and return it as merged value",
        strategy=SumValueStrategy),
    AVG: StrategyRecord(
        id=AVG,
        name="Average of values",
        description="This merge strategy will average all values and return it as merged value",
        strategy=AvgValueStrategy),

    LAST_DATETIME: StrategyRecord(
        id=LAST_DATETIME,
        name="Last date",
        description="This merge strategy works only on date values and it will select last date as merged value",
        strategy=MaxDateTimeStrategy),
    FIRST_DATETIME: StrategyRecord(
        id=FIRST_DATETIME,
        name="First date",
        description="This merge strategy works only on date values and it will select first date as merged value",
        strategy=MinDateTimeStrategy),

    ALWAYS_TRUE: StrategyRecord(
        id=ALWAYS_TRUE,
        name="Always TRUE",
        description="This merge strategy always True as merged value",
        strategy=AlwaysTrueStrategy),
    ALWAYS_FALSE: StrategyRecord(
        id=ALWAYS_FALSE,
        name="Always FALSE",
        description="This merge strategy always False as merged value",
        strategy=AlwaysFalseStrategy),
    AND: StrategyRecord(
        id=AND,
        name="AND Operator",
        description="This merge strategy will use AND operator on all boolean values and will return it as merged value",
        strategy=AndStrategy),
    OR: StrategyRecord(
        id=OR,
        name="OR Operator",
        description="This merge strategy will use OR operator on all boolean values and will return it as merged value",
        strategy=OrStrategy),

    CONCAT: StrategyRecord(
        id=ALWAYS_FALSE,
        name="Concatenate Values",
        description="This merge strategy is used on lists and will concatenate all values and return it as merged value",
        strategy=ConCatStrategy),
    UNIQUE_CONCAT: StrategyRecord(
        id=ALWAYS_FALSE,
        name="Concatenate Unique Values",
        description="This merge strategy is used on lists and will concatenate all values and return unique values it as merged value",
        strategy=ConCatDistinctStrategy),

    FIRST_PROFILE_INSERT_TIME: StrategyRecord(
        id=FIRST_PROFILE_INSERT_TIME,
        name="First Profile Insert Time",
        description="This merge strategy will select the first inserted profile and get the value for the merged field from this profile.",
        strategy=FirstProfileInsertTimeStrategy),

    LAST_PROFILE_UPDATE_TIME: StrategyRecord(
        id=LAST_PROFILE_UPDATE_TIME,
        name="Last Profile Update Time",
        description="This merge strategy will select the last updated profile and get the value for the merged field from this profile.",
        strategy=LastProfileUpdateTimeStrategy),

    LAST_PROFILE_INSERT_TIME: StrategyRecord(
        id=LAST_PROFILE_INSERT_TIME,
        name="Last Profile Insert Time",
        description="This merge strategy will select the last inserted profile and get the value for the merged field from this profile.",
        strategy=LastProfileInsertTimeStrategy),

    FIRST_ITEM: StrategyRecord(
        id=FIRST_ITEM,
        name="No Merging",
        description="This merge strategy will not mereg anything.",
        strategy=FirstStrategy)
}