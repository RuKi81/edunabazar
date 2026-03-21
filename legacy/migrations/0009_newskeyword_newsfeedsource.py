from django.db import migrations, models


def populate_defaults(apps, schema_editor):
    """Populate default keywords and RSS sources from the hardcoded lists."""
    NewsKeyword = apps.get_model('legacy', 'NewsKeyword')
    NewsFeedSource = apps.get_model('legacy', 'NewsFeedSource')

    include_keywords = [
        'сельск', 'аграрн', 'фермер', 'урожай', 'зерн', 'пшениц',
        'кукуруз', 'подсолнеч', 'рапс', 'соя', 'ячмень', 'овёс', 'овес',
        'рожь', 'сахар', 'свёкл', 'свекл', 'картофел', 'овощ', 'фрукт',
        'молок', 'молоч', 'мяс', 'птиц', 'свин', 'говяд', 'баран',
        'рыб', 'аквакультур', 'удобрен', 'пестицид', 'гербицид',
        'комбайн', 'трактор', 'посев', 'уборк', 'уборочн',
        'агро', 'минсельхоз', 'россельхознадзор', 'продовольств',
        'экспорт зерн', 'импорт продовольств', 'животновод',
        'растениевод', 'садовод', 'тепличн', 'парник',
        'хлеб', 'мука', 'корм', 'комбикорм', 'элеватор', 'силос',
        'дойк', 'надо', 'стадо', 'поголовь', 'племен',
        'масло подсолн', 'масло растит', 'маргарин',
        'консерв', 'крупа', 'рис ', 'гречк', 'макарон',
        'колбас', 'сосиск', 'полуфабрикат', 'замороз',
        'кондитер', 'шоколад', 'конфет', 'печень',
        'напиток', 'сок', 'вод', 'пиво', 'вин',
        'чай ', 'кофе', 'какао',
        'орех', 'мёд', 'мед ', 'ягод', 'гриб',
        'специ', 'прянос', 'соус', 'кетчуп', 'майонез',
        'детское питан', 'продукт питан', 'пищев',
        'роспотребнадзор', 'качество продук',
    ]

    exclude_keywords = [
        'медицин', 'лекарств', 'вакцин', 'здоровь', 'больниц', 'клиник',
        'врач', 'пациент', 'диагноз', 'хирург', 'терапевт', 'онколог',
        'коронавирус', 'ковид', 'covid', 'грипп', 'эпидеми', 'пандеми',
        'госпитал', 'стоматолог', 'аптек', 'фармацевт', 'антибиотик',
        'криптовалют', 'биткоин', 'блокчейн',
        'футбол', 'хоккей', 'баскетбол', 'теннис', 'олимпи',
        'кинотеатр', 'сериал', 'актёр', 'актер', 'режиссёр', 'режиссер',
        'смартфон', 'iphone', 'android', 'гаджет',
        'космос', 'nasa', 'роскосмос', 'астроном',
        'военн', 'вооружен', 'артиллер', 'ракетн',
    ]

    for kw in include_keywords:
        NewsKeyword.objects.create(keyword=kw, keyword_type='include', is_active=True)
    for kw in exclude_keywords:
        NewsKeyword.objects.create(keyword=kw, keyword_type='exclude', is_active=True)

    feeds = [
        ('Агроинвестор', 'https://www.agroinvestor.ru/rss/'),
        ('Milknews', 'https://milknews.ru/rss.xml'),
        ('Агровести', 'https://agrovesti.net/rss'),
        ('ТАСС', 'https://tass.ru/rss/v2.xml'),
        ('РБК', 'https://rssexport.rbc.ru/rbcnews/news/30/full.rss'),
        ('Казах-Зерно', 'https://kazakh-zerno.net/rss/'),
    ]
    for name, url in feeds:
        NewsFeedSource.objects.create(name=name, url=url, is_active=True)


class Migration(migrations.Migration):

    dependencies = [
        ('legacy', '0008_news'),
    ]

    operations = [
        migrations.CreateModel(
            name='NewsKeyword',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('keyword', models.CharField(max_length=100, verbose_name='Ключевое слово')),
                ('keyword_type', models.CharField(choices=[('include', 'Включать (тема с/х и продуктов)'), ('exclude', 'Исключать (нерелевантное)')], default='include', max_length=7, verbose_name='Тип')),
                ('is_active', models.BooleanField(default=True, verbose_name='Активно')),
            ],
            options={
                'verbose_name': 'Ключевое слово (новости)',
                'verbose_name_plural': 'Ключевые слова (новости)',
                'db_table': 'news_keyword',
                'ordering': ['keyword_type', 'keyword'],
            },
        ),
        migrations.CreateModel(
            name='NewsFeedSource',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=200, verbose_name='Название')),
                ('url', models.URLField(max_length=500, verbose_name='RSS URL')),
                ('is_active', models.BooleanField(default=True, verbose_name='Активен')),
            ],
            options={
                'verbose_name': 'RSS-источник новостей',
                'verbose_name_plural': 'RSS-источники новостей',
                'db_table': 'news_feed_source',
                'ordering': ['name'],
            },
        ),
        migrations.RunPython(populate_defaults, migrations.RunPython.noop),
    ]
