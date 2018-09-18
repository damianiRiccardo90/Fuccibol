from scrapy.exporters import CsvItemExporter
from Fuccibol.items import Match, Formation, Stats, Event
import csv


# noinspection PyUnusedLocal
class DefaultValuesPipeline(object):
    def process_item(self, item, spider):
        for field in item.fields:
            item.setdefault(field, r'\N')
        return item


# noinspection PyUnusedLocal
# noinspection PyAttributeOutsideInit
class CSVExportPipeline(object):
    def open_spider(self, spider):
        f1 = open('Match.csv', 'wb')
        f2 = open('Formation.csv', 'wb')
        f3 = open('Stats.csv', 'wb')
        f4 = open('Event.csv', 'wb')
        self.match_exporter = CsvItemExporter(f1, quoting=csv.QUOTE_ALL)
        self.formation_exporter = CsvItemExporter(f2, quoting=csv.QUOTE_ALL)
        self.stats_exporter = CsvItemExporter(f3, quoting=csv.QUOTE_ALL)
        self.event_exporter = CsvItemExporter(f4, quoting=csv.QUOTE_ALL)

        self.match_exporter.fields_to_export = [
            'match_id',
            'team_h',
            'team_a',
            'result',
            'league',
            'date',
            'week',
            'kick_off',
            'referee',
            'home_form',
            'away_form'
        ]

        field_names = ['match_id']
        for i in range(1, 24):
            field_names.append('player_h_{0}'.format(i))
        for i in range(1, 24):
            field_names.append('player_h_{0}_num'.format(i))
        for i in range(1, 24):
            field_names.append('player_a_{0}'.format(i))
        for i in range(1, 24):
            field_names.append('player_a_{0}_num'.format(i))
        field_names.append('coach_h')
        field_names.append('coach_a')
        self.formation_exporter.fields_to_export = field_names

        self.stats_exporter.fields_to_export = [
            'match_id',
            'stadium',
            'attendance',
            'capacity',
            'shots_h',
            'shots_a',
            'shots_on_target_h',
            'shots_on_target_a',
            'saves_h',
            'saves_a',
            'corners_h',
            'corners_a',
            'free_kicks_h',
            'free_kicks_a',
            'fouls_h',
            'fouls_a',
            'offsides_h',
            'offsides_a'
        ]

        self.event_exporter.fields_to_export = [
            'match_id',
            'type',
            'sub_type',
            'sub_sub_type',
            'time',
            'player_1',
            'player_2'
        ]

        self.match_exporter.start_exporting()
        self.formation_exporter.start_exporting()
        self.stats_exporter.start_exporting()
        self.event_exporter.start_exporting()

    def close_spider(self, spider):
        self.match_exporter.finish_exporting()
        self.formation_exporter.finish_exporting()
        self.stats_exporter.finish_exporting()
        self.event_exporter.finish_exporting()

    def process_item(self, item, spider):
        if isinstance(item, Match):
            self.match_exporter.export_item(item)
        elif isinstance(item, Formation):
            self.formation_exporter.export_item(item)
        elif isinstance(item, Stats):
            self.stats_exporter.export_item(item)
        elif isinstance(item, Event):
            self.event_exporter.export_item(item)
        return item
