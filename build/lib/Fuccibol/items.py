import scrapy
from scrapy.loader.processors import MapCompose, Compose, TakeFirst
from datetime import datetime
import re


class Match(scrapy.Item):
    match_id = scrapy.Field(output_processor=TakeFirst())
    team_h = scrapy.Field(output_processor=TakeFirst())
    team_a = scrapy.Field(output_processor=TakeFirst())
    result = scrapy.Field(
        input_processor=MapCompose(lambda x: x.strip()),
        output_processor=TakeFirst()
    )
    league = scrapy.Field(output_processor=TakeFirst())
    date = scrapy.Field(
        input_processor=MapCompose(
            lambda x: datetime.strptime(x, '%a, %b %d, %Y').strftime('%Y-%m-%d')
        ),
        output_processor=TakeFirst()
    )
    week = scrapy.Field(
        input_processor=MapCompose(lambda x: re.match(r'\d', x).group()),
        output_processor=TakeFirst()
    )
    kick_off = scrapy.Field(
        input_processor=MapCompose(lambda x: re.search(r'\d+:\d+', x).group()),
        output_processor=TakeFirst()
    )
    referee = scrapy.Field(output_processor=TakeFirst())
    home_form = scrapy.Field(
        input_processor=MapCompose(
            lambda x: x.strip(),
            lambda x: re.search(r'.+: (.+)', x).group(1)
        ),
        output_processor=TakeFirst()
    )
    away_form = scrapy.Field(
        input_processor=MapCompose(
            lambda x: x.strip(),
            lambda x: re.search(r'.+: (.+)', x).group(1)
        ),
        output_processor=TakeFirst()
    )


class Formation(scrapy.Item):
    match_id = scrapy.Field()
    player_h_1 = scrapy.Field()
    player_h_2 = scrapy.Field()
    player_h_3 = scrapy.Field()
    player_h_4 = scrapy.Field()
    player_h_5 = scrapy.Field()
    player_h_6 = scrapy.Field()
    player_h_7 = scrapy.Field()
    player_h_8 = scrapy.Field()
    player_h_9 = scrapy.Field()
    player_h_10 = scrapy.Field()
    player_h_11 = scrapy.Field()
    player_h_12 = scrapy.Field()
    player_h_13 = scrapy.Field()
    player_h_14 = scrapy.Field()
    player_h_15 = scrapy.Field()
    player_h_16 = scrapy.Field()
    player_h_17 = scrapy.Field()
    player_h_18 = scrapy.Field()
    player_h_19 = scrapy.Field()
    player_h_20 = scrapy.Field()
    player_h_21 = scrapy.Field()
    player_h_22 = scrapy.Field()
    player_h_23 = scrapy.Field()
    player_h_1_num = scrapy.Field()
    player_h_2_num = scrapy.Field()
    player_h_3_num = scrapy.Field()
    player_h_4_num = scrapy.Field()
    player_h_5_num = scrapy.Field()
    player_h_6_num = scrapy.Field()
    player_h_7_num = scrapy.Field()
    player_h_8_num = scrapy.Field()
    player_h_9_num = scrapy.Field()
    player_h_10_num = scrapy.Field()
    player_h_11_num = scrapy.Field()
    player_h_12_num = scrapy.Field()
    player_h_13_num = scrapy.Field()
    player_h_14_num = scrapy.Field()
    player_h_15_num = scrapy.Field()
    player_h_16_num = scrapy.Field()
    player_h_17_num = scrapy.Field()
    player_h_18_num = scrapy.Field()
    player_h_19_num = scrapy.Field()
    player_h_20_num = scrapy.Field()
    player_h_21_num = scrapy.Field()
    player_h_22_num = scrapy.Field()
    player_h_23_num = scrapy.Field()
    player_a_1 = scrapy.Field()
    player_a_2 = scrapy.Field()
    player_a_3 = scrapy.Field()
    player_a_4 = scrapy.Field()
    player_a_5 = scrapy.Field()
    player_a_6 = scrapy.Field()
    player_a_7 = scrapy.Field()
    player_a_8 = scrapy.Field()
    player_a_9 = scrapy.Field()
    player_a_10 = scrapy.Field()
    player_a_11 = scrapy.Field()
    player_a_12 = scrapy.Field()
    player_a_13 = scrapy.Field()
    player_a_14 = scrapy.Field()
    player_a_15 = scrapy.Field()
    player_a_16 = scrapy.Field()
    player_a_17 = scrapy.Field()
    player_a_18 = scrapy.Field()
    player_a_19 = scrapy.Field()
    player_a_20 = scrapy.Field()
    player_a_21 = scrapy.Field()
    player_a_22 = scrapy.Field()
    player_a_23 = scrapy.Field()
    player_a_1_num = scrapy.Field()
    player_a_2_num = scrapy.Field()
    player_a_3_num = scrapy.Field()
    player_a_4_num = scrapy.Field()
    player_a_5_num = scrapy.Field()
    player_a_6_num = scrapy.Field()
    player_a_7_num = scrapy.Field()
    player_a_8_num = scrapy.Field()
    player_a_9_num = scrapy.Field()
    player_a_10_num = scrapy.Field()
    player_a_11_num = scrapy.Field()
    player_a_12_num = scrapy.Field()
    player_a_13_num = scrapy.Field()
    player_a_14_num = scrapy.Field()
    player_a_15_num = scrapy.Field()
    player_a_16_num = scrapy.Field()
    player_a_17_num = scrapy.Field()
    player_a_18_num = scrapy.Field()
    player_a_19_num = scrapy.Field()
    player_a_20_num = scrapy.Field()
    player_a_21_num = scrapy.Field()
    player_a_22_num = scrapy.Field()
    player_a_23_num = scrapy.Field()
    coach_h = scrapy.Field()
    coach_a = scrapy.Field()


class Stats(scrapy.Item):
    match_id = scrapy.Field()
    stadium = scrapy.Field()
    attendance = scrapy.Field(
        input_processor=MapCompose(lambda x: x.replace('.', ''))
    )
    capacity = scrapy.Field(
        input_processor=MapCompose(lambda x: x.replace('.', ''))
    )
    shots_h = scrapy.Field()
    shots_a = scrapy.Field()
    shots_on_target_h = scrapy.Field()
    shots_on_target_a = scrapy.Field()
    saves_h = scrapy.Field()
    saves_a = scrapy.Field()
    corners_h = scrapy.Field()
    corners_a = scrapy.Field()
    free_kicks_h = scrapy.Field()
    free_kicks_a = scrapy.Field()
    fouls_h = scrapy.Field()
    fouls_a = scrapy.Field()
    offsides_h = scrapy.Field()
    offsides_a = scrapy.Field()


def event_num_remove(x, loader_context):
    if loader_context.get('type') == 'Goal':
        return None if re.match(r'\d', x) else x
    else:
        return x


class Event(scrapy.Item):
    match_id = scrapy.Field(output_processor=TakeFirst())
    type = scrapy.Field(output_processor=TakeFirst())
    sub_type = scrapy.Field(
        input_processor=MapCompose(
            lambda x: x.strip(),
            event_num_remove,
            lambda x: x if x else None
        ),
        output_processor=TakeFirst()
    )
    sub_sub_type = scrapy.Field(
        input_processor=MapCompose(
            lambda x: x.strip(),
            event_num_remove,
            lambda x: x if x else None
        ),
        output_processor=TakeFirst()
    )
    time = scrapy.Field(
        input_processor=MapCompose(lambda x: int(x)),
        output_processor=Compose(
            lambda x: (x[0] + 10 * x[1] + 36) // 36,
            lambda x: x if x <= 120 else None
        )
    )
    player_1 = scrapy.Field(output_processor=TakeFirst())
    player_2 = scrapy.Field(output_processor=TakeFirst())
