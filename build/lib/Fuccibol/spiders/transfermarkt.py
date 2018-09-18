from scrapy.spiders import Spider, Request
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst
from Fuccibol.items import Match, Formation, Stats, Event
import re
# scrapy shell -s USER_AGENT='Mediapartners-Google' 'url'


class TransfermarktSpider(Spider):
    name = 'transfermarkt'
    allowed_domains = ['transfermarkt.co.uk']

    def start_requests(self):
        league_name = ('serie-a', '1-bundesliga', 'ligue-1', 'laliga', 'premier-league', 'eredivisie')
        league_abbr = ('IT1', 'L1', 'FR1', 'ES1', 'GB1', 'NL1')
        league_weeks = (38, 34, 38, 38, 38, 34)

        url_str = 'https://www.transfermarkt.co.uk/{0}/gesamtspielplan/wettbewerb' \
                  '/{1}?saison_id={2}&spieltagVon=1&spieltagBis={3}'

        for i in range(6):
            for j in range(11):
                yield Request(url=url_str.format(league_name[i], league_abbr[i], 1996 + j, league_weeks[i]))

    def parse(self, response):
        relative_urls = response.xpath('//a[@title="Match sheet"]/@href').extract()
        for url in relative_urls:
            yield Request('https://www.transfermarkt.co.uk' + url, callback=self._parse_match)

    def _parse_match(self, response):
        match_id = re.search(r'(\d+)$', response.request.url).group()
        home = response.xpath('//div[contains(@class, "sb-heim")]//a[@class="sb-vereinslink"]/text()').extract_first()
        away = response.xpath('//div[contains(@class, "sb-gast")]//a[@class="sb-vereinslink"]/text()').extract_first()
        result = response.xpath('//div[@class="sb-endstand"]/text()').extract_first()
        league = response.xpath('//div[@class="spielername-profil"]//a/text()').extract_first()
        date = response.xpath('//div[@class="sb-spieldaten"]//a[2]/text()').extract_first()
        week = response.xpath('//div[@class="sb-spieldaten"]//a[1]/text()').extract_first()
        kick_off = response.xpath('//*[@id="main"]/div[8]/div/div/div[2]/div[3]/p[1]/text()[2]').extract_first()
        referee = response.xpath('//p[@class="sb-zusatzinfos"]/a/text()').extract_first()
        home_form = response.xpath('//*[@id="main"]/div[13]/div/div/div[2]/div[2]/div[1]/text()').extract_first()
        away_form = response.xpath('//*[@id="main"]/div[13]/div/div/div[3]/div[2]/div[1]/text()').extract_first()

        loader = ItemLoader(item=Match(), response=response)
        loader.add_value('match_id', match_id)
        loader.add_value('team_h', home)
        loader.add_value('team_a', away)
        loader.add_value('result', result)
        loader.add_value('league', league)
        loader.add_value('date', date)
        loader.add_value('week', week)
        loader.add_value('kick_off', kick_off)
        loader.add_value('referee', referee)
        loader.add_value('home_form', home_form)
        loader.add_value('away_form', away_form)
        yield loader.load_item()

        goal_list = response.xpath('//*[@id="sb-tore"]/ul/li')
        for i in range(len(goal_list)):
            loader = ItemLoader(item=Event(), response=response, type='Goal')
            loader.add_value('match_id', match_id)
            loader.add_value('type', 'Goal')
            sub_type = goal_list[i].xpath('./div/div[4]/a[1]/following-sibling::text()').extract_first().split(',')
            loader.add_value('sub_type', sub_type)
            time = goal_list[i].xpath('./div/div[1]/span/@style').re(r'\d+')
            loader.add_value('time', time)
            player_1 = goal_list[i].xpath('./div/div[4]/a[1]/text()').extract_first()
            loader.add_value('player_1', player_1)
            player_2 = goal_list[i].xpath('./div/div[4]/a[2]/text()').extract_first()
            if player_2 is not None:
                loader.add_value('player_2', player_2)
                sub_sub_type = goal_list[i].xpath('./div/div[4]/a[2]/following-sibling::text()') \
                    .extract_first().split(',')
                loader.add_value('sub_sub_type', sub_sub_type)
            yield loader.load_item()

        sub_list = response.xpath('//*[@id="sb-wechsel"]/ul/li')
        for i in range(len(sub_list)):
            loader = ItemLoader(item=Event(), response=response, type='Substitution')
            loader.add_value('match_id', match_id)
            loader.add_value('type', 'Substitution')
            sub_type = sub_list[i].xpath('./div/div[4]/span[2]/span[1]/text()').extract_first().split(',')
            loader.add_value('sub_type', sub_type)
            time = sub_list[i].xpath('./div/div[1]/span/@style').re(r'\d+')
            loader.add_value('time', time)
            player_1 = sub_list[i].xpath('./div/div[4]/span[@class="sb-aktion-wechsel-aus"]/a/text()').extract_first()
            loader.add_value('player_1', player_1)
            player_2 = sub_list[i].xpath('./div/div[4]/span[@class="sb-aktion-wechsel-ein"]/a/text()').extract_first()
            loader.add_value('player_2', player_2)
            yield loader.load_item()

        card_list = response.xpath('//*[@id="sb-karten"]/ul/li')
        for i in range(len(card_list)):
            loader = ItemLoader(item=Event(), response=response, type='Card')
            loader.add_value('match_id', match_id)
            loader.add_value('type', 'Card')
            type_list = card_list[i].xpath('./div/div[4]/br/following-sibling::text()').extract_first().split(',')
            loader.add_value('sub_type', type_list[0])
            loader.add_value('sub_sub_type', type_list[1] if len(type_list) > 1 else None)
            time = card_list[i].xpath('./div/div[1]/span/@style').re(r'\d+')
            loader.add_value('time', time)
            player_1 = card_list[i].xpath('./div/div[4]/a/text()').extract_first()
            loader.add_value('player_1', player_1)
            yield loader.load_item()

        relative_url = response.xpath('//li[@id="Lineup"]/a/@href').extract_first()
        yield Request('https://www.transfermarkt.co.uk' + relative_url, callback=self._parse_formation)
        relative_url = response.xpath('//li[@id="Statistics"]/a/@href').extract_first()
        yield Request('https://www.transfermarkt.co.uk' + relative_url, callback=self._parse_stats)

    def _parse_formation(self, response):
        match_id = re.search(r'(\d+)$', response.request.url).group()

        player_h_names = []
        player_a_names = []
        player_h_nums = []
        player_a_nums = []

        home_form = response.xpath('(//div[@class="row sb-formation"])[1]/div[1]//table[@class="items"]')
        away_form = response.xpath('(//div[@class="row sb-formation"])[1]/div[2]//table[@class="items"]')
        for i in range(1, 12):
            player_h_names += [home_form.xpath('.//tr[{0}]//a[@class="wichtig"]/text()'.format(i)).extract_first()]
            player_a_names += [away_form.xpath('.//tr[{0}]//a[@class="wichtig"]/text()'.format(i)).extract_first()]
            player_h_nums += [
                home_form.xpath('.//tr[{0}]//div[@class="rn_nummer"]/text()'.format(i)).extract_first()]
            player_a_nums += [
                away_form.xpath('.//tr[{0}]//div[@class="rn_nummer"]/text()'.format(i)).extract_first()]

        home_bench = response.xpath('(//div[@class="row sb-formation"])[2]/div[1]//table[@class="items"]')
        away_bench = response.xpath('(//div[@class="row sb-formation"])[2]/div[2]//table[@class="items"]')
        bench_num = len(home_bench.xpath('./tr'))
        for i in range(1, bench_num + 1):
            player_h_names += [home_bench.xpath('.//tr[{0}]//a[@class="wichtig"]/text()'.format(i)).extract_first()]
            player_a_names += [away_bench.xpath('.//tr[{0}]//a[@class="wichtig"]/text()'.format(i)).extract_first()]
            player_h_nums += [
                home_bench.xpath('.//tr[{0}]//div[@class="rn_nummer"]/text()'.format(i)).extract_first()]
            player_a_nums += [
                away_bench.xpath('.//tr[{0}]//div[@class="rn_nummer"]/text()'.format(i)).extract_first()]

        home_coach = response.xpath('(//div[@class="row sb-formation"])[3]/div[1]//table[@class="items"]')
        away_coach = response.xpath('(//div[@class="row sb-formation"])[3]/div[2]//table[@class="items"]')
        coach_h = home_coach.xpath('.//tr[1]//a[@class="wichtig"]/text()').extract_first()
        coach_a = away_coach.xpath('.//tr[1]//a[@class="wichtig"]/text()').extract_first()

        loader = ItemLoader(item=Formation(), response=response)
        loader.default_output_processor = TakeFirst()
        loader.add_value('match_id', match_id)
        for i in range(1, 12 + bench_num):
            loader.add_value('player_h_{0}{1}'.format(i, ''), player_h_names[i - 1])
            loader.add_value('player_a_{0}{1}'.format(i, ''), player_a_names[i - 1])
            loader.add_value('player_h_{0}{1}'.format(i, '_num'), player_h_nums[i - 1])
            loader.add_value('player_a_{0}{1}'.format(i, '_num'), player_a_nums[i - 1])
        loader.add_value('coach_h', coach_h)
        loader.add_value('coach_a', coach_a)
        return loader.load_item()

    def _parse_stats(self, response):
        # TODO Mancano le percentuali di possesso palla!!
        match_id = re.search(r'(\d+)$', response.request.url).group()
        stadium = response.xpath('//th[.="Stadium:"]/following-sibling::td/a/text()').extract_first()
        attendance = response.xpath('//th[.="Attendance:"]/following-sibling::td/text()').extract_first()
        capacity = response.xpath('//th[.="Available capacity:"]/following-sibling::td/text()').extract_first()
        xpath = '//*[@id="main"]/div[13]/div[1]/div/div[{0}]/ul/li[{1}]/div/div[2]/text()'
        shots_h = response.xpath(xpath.format(5, 1)).extract_first()
        shots_a = response.xpath(xpath.format(5, 2)).extract_first()
        shots_on_target_h = response.xpath(xpath.format(7, 1)).extract_first()
        shots_on_target_a = response.xpath(xpath.format(7, 2)).extract_first()
        saves_h = response.xpath(xpath.format(11, 1)).extract_first()
        saves_a = response.xpath(xpath.format(11, 2)).extract_first()
        corners_h = response.xpath(xpath.format(13, 1)).extract_first()
        corners_a = response.xpath(xpath.format(13, 2)).extract_first()
        free_kicks_h = response.xpath(xpath.format(15, 1)).extract_first()
        free_kicks_a = response.xpath(xpath.format(15, 2)).extract_first()
        fouls_h = response.xpath(xpath.format(17, 1)).extract_first()
        fouls_a = response.xpath(xpath.format(17, 2)).extract_first()
        offsides_h = response.xpath(xpath.format(19, 1)).extract_first()
        offsides_a = response.xpath(xpath.format(19, 2)).extract_first()

        loader = ItemLoader(item=Stats(), response=response)
        loader.default_output_processor = TakeFirst()
        loader.add_value('match_id', match_id)
        loader.add_value('stadium', stadium)
        loader.add_value('attendance', attendance)
        loader.add_value('capacity', capacity)
        loader.add_value('shots_h', shots_h)
        loader.add_value('shots_a', shots_a)
        loader.add_value('shots_on_target_h', shots_on_target_h)
        loader.add_value('shots_on_target_a', shots_on_target_a)
        loader.add_value('saves_h', saves_h)
        loader.add_value('saves_a', saves_a)
        loader.add_value('corners_h', corners_h)
        loader.add_value('corners_a', corners_a)
        loader.add_value('free_kicks_h', free_kicks_h)
        loader.add_value('free_kicks_a', free_kicks_a)
        loader.add_value('fouls_h', fouls_h)
        loader.add_value('fouls_a', fouls_a)
        loader.add_value('offsides_h', offsides_h)
        loader.add_value('offsides_a', offsides_a)
        return loader.load_item()
